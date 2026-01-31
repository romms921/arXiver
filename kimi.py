import pandas as pd
import requests
import tarfile
import os
import shutil
import time
import re
import json
from openai import OpenAI
from tqdm import tqdm
from dotenv import load_dotenv

# API Configuration
load_dotenv()
API_KEY = os.getenv("KIMI_API")
BASE_URL = "https://api.moonshot.ai/v1"

client = OpenAI(
    api_key=API_KEY,
    base_url=BASE_URL,
)

# Configuration
DATASET_PATH = '2025_Data.csv'
OUTPUT_CSV = 'kimi_output.csv'
OUTPUT_JSON = 'kimi_output.json'
ARXIV_DIR = 'arxiv_papers'
START_INDEX = 0
STOP_INDEX = 1000000  # Run until end or manually stopped

def setup_directories():
    if not os.path.exists(ARXIV_DIR):
        os.makedirs(ARXIV_DIR)
    
    # Ensure output CSV has header if new
    if not os.path.exists(OUTPUT_CSV):
        pd.DataFrame(columns=['original_index', 'arxiv_id', 'extracted_authors', 'extracted_affiliations', 'extracted_countries', 'first_author_country']).to_csv(OUTPUT_CSV, index=False)

def get_eprint_url(pdf_link):
    """Converts a PDF link (e.g., https://arxiv.org/pdf/2501.13049) to an e-print source link."""
    if not isinstance(pdf_link, str):
        return None, None
    
    # Extract ID
    match = re.search(r'arxiv.org/pdf/([\d\.]+)', pdf_link)
    if not match:
        match = re.search(r'arxiv.org/abs/([\d\.]+)', pdf_link)
    
    if match:
        arxiv_id = match.group(1)
        return f"https://arxiv.org/e-print/{arxiv_id}", arxiv_id
    return None, None

def download_source(url, arxiv_id):
    """Downloads the source file from arXiv."""
    print(f"Downloading source for {arxiv_id} from {url}...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
    }
    try:
        response = requests.get(url, stream=True, headers=headers)
        if response.status_code == 200:
            file_path = os.path.join(ARXIV_DIR, f"{arxiv_id}.tar.gz")
            with open(file_path, 'wb') as f:
                f.write(response.content)
            return file_path
        else:
            print(f"Failed to download {url}: Status {response.status_code}")
            return None
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return None

def extract_and_read_latex(file_path, arxiv_id):
    """Extracts tar.gz and reads LaTeX files."""
    extract_dir = os.path.join(ARXIV_DIR, arxiv_id)
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
        
    try:
        is_tar = tarfile.is_tarfile(file_path)
    except:
        is_tar = False

    try:
        if is_tar:
            with tarfile.open(file_path, "r:gz") as tar:
                tar.extractall(path=extract_dir)
        else:
            # Maybe it's a single .tex file rename?
            single_tex = os.path.join(extract_dir, f"{arxiv_id}.tex")
            shutil.copy(file_path, single_tex)
            
        # Find .tex files
        tex_files = []
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.endswith(".tex") or file.endswith(".ltx"):
                    tex_files.append(os.path.join(root, file))
        
        # Sort by size to find main file
        tex_files.sort(key=lambda x: os.path.getsize(x), reverse=True)
        
        # Look for explicit author content
        for tex_file in tex_files:
            try:
                with open(tex_file, 'r', errors='replace') as f:
                    content = f.read()
                    if '\\author' in content or '\\address' in content or '\\affiliation' in content or '\\documentclass' in content:
                        print(f"Found LaTeX content in {os.path.basename(tex_file)} ({len(content)} chars)")
                        return content
            except:
                continue
                
        # Fallback to largest
        if tex_files:
            print("No obvious main file found, returning largest.")
            with open(tex_files[0], 'r', errors='replace') as f:
                return f.read()
                
    except Exception as e:
        print(f"Error extracting/reading {file_path}: {e}")
        
    return None

def cleanup(arxiv_id):
    """Deletes downloaded files."""
    try:
        file_path = os.path.join(ARXIV_DIR, f"{arxiv_id}.tar.gz")
        extract_dir = os.path.join(ARXIV_DIR, arxiv_id)
        
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(extract_dir):
            shutil.rmtree(extract_dir)
    except Exception as e:
        print(f"Error cleaning up {arxiv_id}: {e}")

def query_kimi(latex_text):
    """Sends LaTeX content to Kimi API."""
    prompt = """
From this LaTeX extract all the author names and their corresponding affiliations. Preserve the order of the authors and account for the fact that one author can have multiple affiliations. Also, extract the country associated with each affiliation as well as the country ot countires associated with the first author. Give the output as json with the authors their respective affiliations and country and finally a first author country/countries.

Output format should be a JSON object ONLY:
{
  "authors": [
    {
      "name": "Author Name",
      "affiliations": ["Affiliation 1", "Affiliation 2"],
      "countries": ["Country 1", "Country 2"]
    }
  ],
  "first_author_countries": ["Country 1"]
}
"""
    # Truncate to avoid context limit if necessary
    truncated_latex = latex_text

    try:
        completion = client.chat.completions.create(
            model="kimi-k2-turbo-preview",  # Using user specific model
            messages=[
                {"role": "system", "content": "You are a helpful assistant that extracts structured data from LaTeX."},
                {"role": "user", "content": f"{prompt}\n\nLaTeX Content:\n{truncated_latex}"}
            ],
            temperature=0.3,
        )
        return completion.choices[0].message.content
    except Exception as e:
        print(f"API Error: {e}")
        return None

def parse_kimi_response(response_text):
    """Parses the JSON response from Kimi."""
    try:
        if not response_text:
            return None
        start = response_text.find('{')
        end = response_text.rfind('}') + 1
        if start == -1 or end == 0:
            return None
            
        json_str = response_text[start:end]
        data = json.loads(json_str)
        return data
    except Exception as e:
        print(f"JSON Parse Error: {e}")
        return None

def format_for_csv(json_data):
    if not json_data:
        return '[]', '[]', '[]', '[]'
        
    authors = []
    affiliations = []
    countries = []
    
    if "authors" in json_data and isinstance(json_data["authors"], list):
        for author in json_data["authors"]:
            authors.append(author.get("name", ""))
            
            affs = author.get("affiliations", [])
            if isinstance(affs, list):
                # Clean up affiliations
                clean_affs = [str(x).replace('\n', ' ').strip() for x in affs]
                affiliations.append("; ".join(clean_affs))
            else:
                affiliations.append(str(affs).replace('\n', ' ').strip())
            
            cnts = author.get("countries", [])
            if isinstance(cnts, list):
                clean_cnts = [str(x).replace('\n', ' ').strip() for x in cnts]
                countries.append("; ".join(clean_cnts))
            else:
                countries.append(str(cnts).replace('\n', ' ').strip())
                
    first_author_country = json_data.get("first_author_countries", [])
    if isinstance(first_author_country, list):
        first_author_country = "; ".join([str(x).replace('\n', ' ').strip() for x in first_author_country])
    else:
        first_author_country = str(first_author_country).replace('\n', ' ').strip()
    
    return json.dumps(authors), json.dumps(affiliations), json.dumps(countries), first_author_country

def main():
    setup_directories()
    
    print(f"Reading dataset {DATASET_PATH}...")
    df = pd.read_csv(DATASET_PATH)
    
    # Identify link column
    link_col = None
    for col in df.columns:
        if 'pdf_link' in col:
            link_col = col
            break
        if link_col is None and len(df) > 0:
             val = str(df[col].iloc[0])
             if 'arxiv.org' in val:
                 link_col = col

    if not link_col:
        print("Could not find pdf_link column")
        return
        
    print(f"Processing with link column: {link_col}")
    
    processed_indices = set()
    if os.path.exists(OUTPUT_CSV):
        try:
            existing = pd.read_csv(OUTPUT_CSV)
            if 'original_index' in existing.columns:
                processed_indices = set(existing['original_index'].tolist())
                print(f"Found {len(processed_indices)} already processed entries.")
        except:
            pass

    # Calculate subset to process
    subset = df.iloc[START_INDEX:STOP_INDEX]
    
    # Filter out already processed entries
    subset_to_process = subset[~subset.index.isin(processed_indices)]
    
    if subset_to_process.empty:
        print("No new entries to process.")
        return

    pbar = tqdm(subset_to_process.iterrows(), total=len(subset_to_process), desc="Processing papers")
    for index, row in pbar:
        pdf_link = row[link_col]
        eprint_url, arxiv_id = get_eprint_url(pdf_link)
        
        if not eprint_url:
            tqdm.write(f"[{index}] No valid e-print URL for {arxiv_id or 'unknown'}")
            continue
            
        file_path = download_source(eprint_url, arxiv_id)
        if not file_path:
            time.sleep(3)
            continue
            
        latex_text = extract_and_read_latex(file_path, arxiv_id)
        
        if latex_text:
            tqdm.write(f"[{index}] Querying API for {arxiv_id}...")
            response = query_kimi(latex_text)
            
            json_data = parse_kimi_response(response)
            
            if json_data:
                # Log JSON
                with open(OUTPUT_JSON, 'a') as f:
                    entry = {"index": index, "data": json_data}
                    f.write(json.dumps(entry) + "\n")
                    
                authors, affils, countries, first_country = format_for_csv(json_data)
                
                # Append to CSV
                res_df = pd.DataFrame([{
                    'original_index': index,
                    'arxiv_id': arxiv_id,
                    'extracted_authors': authors,
                    'extracted_affiliations': affils,
                    'extracted_countries': countries,
                    'first_author_country': first_country
                }])
                res_df.to_csv(OUTPUT_CSV, mode='a', header=False, index=False)
                tqdm.write(f"[{index}] Saved result for {arxiv_id}")
            else:
                tqdm.write(f"[{index}] Failed to parse response for {arxiv_id}")
        else:
            tqdm.write(f"[{index}] No latex text returned for {arxiv_id}")
        
        cleanup(arxiv_id)
        time.sleep(3)

if __name__ == "__main__":
    main()
