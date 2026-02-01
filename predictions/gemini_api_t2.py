"""
Gemini API-based Author Affiliation Extractor
Uses Gemini for batch processing of papers with structured JSON output.
"""

import json
import logging
import os
import re
import shutil
import tarfile
import time
from datetime import datetime
from typing import List, Optional

import dotenv
import pandas as pd
import requests
from google import genai
from pydantic import BaseModel, Field
from tqdm import tqdm


# =======================
# PYDANTIC MODELS FOR STRUCTURED OUTPUT
# =======================
class Author(BaseModel):
    """Model for a single author with affiliations."""
    name: str = Field(description="Full name of the author.")
    affiliations: List[str] = Field(default_factory=list, description="List of affiliations for this author.")
    countries: List[str] = Field(default_factory=list, description="List of countries associated with the affiliations.")


class Paper(BaseModel):
    """Model for a single paper's extracted data."""
    arxiv_id: str = Field(description="The arXiv ID of the paper.")
    authors: List[Author] = Field(default_factory=list, description="List of authors with their affiliations.")
    first_author_countries: List[str] = Field(default_factory=list, description="Countries of the first author.")


class BatchResponse(BaseModel):
    """Model for the batch response containing multiple papers."""
    papers: List[Paper] = Field(description="List of papers with extracted author and affiliation data.")

# =======================
# CONFIGURATION
# =======================
MODEL_ID = "gemini-2.5-flash-lite"
CSV_PATH = "2025_Data.csv"
OUTPUT_CSV_PATH = "gemini_affil_output.csv"

ARXIV_DIR = "arxiv_papers"
OUTPUT_JSON = "gemini_output.json"

# Batch configuration
BATCH_SIZE = 20  # Number of papers per batch
MAX_LATEX_CHARS = 15000  # Max chars of LaTeX per paper

# Rate limiting for free tier: 15 requests per minute
RATE_LIMIT_SECONDS = 60.0 / 15  # ~4 seconds between requests
MAX_RETRIES = 3
RETRY_DELAY = 60  # seconds to wait on quota error

# =======================
# LOGGING SETUP
# =======================
def setup_logging():
    """Configure logging with both file and console handlers."""
    log_filename = f"gemini_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Create logger
    logger = logging.getLogger("GeminiExtractor")
    logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers
    logger.handlers = []
    
    # File handler - detailed logging
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)
    
    # Console handler - info level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(levelname)-8s | %(message)s')
    console_handler.setFormatter(console_format)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# =======================
# INIT GEMINI CLIENT
# =======================
dotenv.load_dotenv()

client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))


# =======================
# UTILITY FUNCTIONS
# =======================
def setup_directories():
    """Create necessary directories."""
    if not os.path.exists(ARXIV_DIR):
        os.makedirs(ARXIV_DIR)
        logger.info(f"Created directory: {ARXIV_DIR}")



def get_eprint_url(pdf_link):
    """Converts a PDF link to an e-print source link."""
    if not isinstance(pdf_link, str):
        return None, None

    match = re.search(r"arxiv.org/pdf/([\d\.]+)", pdf_link)
    if not match:
        match = re.search(r"arxiv.org/abs/([\d\.]+)", pdf_link)

    if match:
        arxiv_id = match.group(1)
        return f"https://arxiv.org/e-print/{arxiv_id}", arxiv_id
    return None, None


def download_source(url, arxiv_id):
    """Downloads the source file from arXiv."""
    logger.debug(f"Downloading source for {arxiv_id} from {url}")
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            file_path = os.path.join(ARXIV_DIR, f"{arxiv_id}.tar.gz")
            with open(file_path, "wb") as f:
                f.write(response.content)
            return file_path
        else:
            logger.warning(f"Failed to download {url}: Status {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
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
                with open(tex_file, "r", errors="replace") as f:
                    content = f.read()
                    if (
                        "\\author" in content
                        or "\\address" in content
                        or "\\affiliation" in content
                        or "\\documentclass" in content
                    ):
                        logger.debug(f"Found LaTeX in {os.path.basename(tex_file)} ({len(content)} chars)")
                        return content
            except:
                continue

        # Fallback to largest
        if tex_files:
            with open(tex_files[0], "r", errors="replace") as f:
                return f.read()

    except Exception as e:
        logger.error(f"Error extracting/reading {file_path}: {e}")

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
        logger.error(f"Error cleaning up {arxiv_id}: {e}")


# =======================
# GEMINI API FUNCTIONS
# =======================
def build_batch_prompt(papers_data):
    """
    Build a prompt for a batch of papers.
    papers_data: list of dicts with 'arxiv_id', 'title', 'latex_text'
    """
    system_prompt = """You are a helpful assistant that extracts structured author and affiliation data from LaTeX documents.

For each paper provided:
1. Extract all author names in order
2. For each author, extract their affiliations (some authors have multiple)
3. For each affiliation, identify the country
4. Identify the first author's country/countries

IMPORTANT: Return ONLY valid JSON matching the exact schema provided. Do not include any LaTeX commands or special characters in your output - convert them to plain text.
"""

    # Build the papers section
    papers_section = ""
    for i, paper in enumerate(papers_data, 1):
        # Just truncate the LaTeX - no filtering
        truncated_latex = paper['latex_text'][:MAX_LATEX_CHARS]
        
        papers_section += f"""
--- PAPER {i} ---
ArXiv ID: {paper['arxiv_id']}
Title: {paper.get('title', 'Unknown')}

LaTeX Content:
{truncated_latex}

"""

    user_prompt = f"""
Extract author and affiliation information from these papers:

{papers_section}

Return a JSON object with this exact structure:
{{
  "papers": [
    {{
      "arxiv_id": "the_arxiv_id",
      "authors": [
        {{
          "name": "Author Full Name",
          "affiliations": ["Institution 1", "Institution 2"],
          "countries": ["Country1", "Country2"]
        }}
      ],
      "first_author_countries": ["Country"]
    }}
  ]
}}
"""
    
    return system_prompt, user_prompt


def clean_json_response(response_text: str) -> str:
    """
    Clean up JSON response to fix common escape character issues.
    LaTeX content often contains backslashes that break JSON parsing.
    """
    if not response_text:
        return response_text
    
    # Remove markdown code blocks if present
    if response_text.startswith("```json"):
        response_text = response_text[7:]
    if response_text.startswith("```"):
        response_text = response_text[3:]
    if response_text.endswith("```"):
        response_text = response_text[:-3]
    
    response_text = response_text.strip()
    
    # Fix common LaTeX escape issues in JSON strings
    # These patterns appear inside JSON string values and break parsing
    
    # Replace problematic backslash sequences that aren't valid JSON escapes
    # Valid JSON escapes: \", \\, \/, \b, \f, \n, \r, \t, \uXXXX
    
    # First, let's try to parse as-is
    try:
        json.loads(response_text)
        return response_text  # It's already valid
    except json.JSONDecodeError:
        pass
    
    # Try fixing common issues
    # Replace single backslashes (not followed by valid escape char) with double backslashes
    import re as regex_module
    
    # This regex finds backslashes not followed by valid JSON escape characters
    # and not already doubled
    fixed_text = response_text
    
    # Common LaTeX commands that cause issues - escape the backslash
    latex_commands = [
        r'\\textsuperscript', r'\\textsubscript', r'\\emph', r'\\textit', r'\\textbf',
        r'\\LaTeX', r'\\TeX', r'\\cite', r'\\ref', r'\\label',
        r'\\alpha', r'\\beta', r'\\gamma', r'\\delta', r'\\epsilon',
        r'\\times', r'\\cdot', r'\\pm', r'\\sim', r'\\approx',
        r'\\&', r'\\%', r'\\$', r'\\#', r'\\_',
    ]
    
    for cmd in latex_commands:
        # Replace \command with \\command in the JSON
        pattern = cmd.replace(r'\\', r'\\')
        replacement = cmd
        fixed_text = fixed_text.replace(pattern.replace(r'\\', '\\'), replacement)
    
    # More aggressive: replace any backslash followed by a letter with escaped version
    # But only inside string values
    def fix_backslashes_in_strings(text):
        """Fix backslashes inside JSON string values."""
        result = []
        in_string = False
        i = 0
        while i < len(text):
            char = text[i]
            
            if char == '"' and (i == 0 or text[i-1] != '\\'):
                in_string = not in_string
                result.append(char)
            elif char == '\\' and in_string:
                # Check if this is already a valid JSON escape
                if i + 1 < len(text):
                    next_char = text[i + 1]
                    if next_char in '"\\bfnrtu/':
                        # Valid JSON escape, keep as-is
                        result.append(char)
                    else:
                        # Invalid escape, double the backslash
                        result.append('\\\\')
                else:
                    result.append('\\\\')
            else:
                result.append(char)
            i += 1
        return ''.join(result)
    
    fixed_text = fix_backslashes_in_strings(fixed_text)
    
    return fixed_text


def query_gemini_batch(papers_data: list, retry_count: int = 0) -> Optional[BatchResponse]:
    """
    Send a batch of papers to Gemini and get structured extraction results.
    Uses Pydantic schema for reliable JSON parsing.
    Returns BatchResponse or None on failure.
    """
    if not papers_data:
        return None
    
    # Log batch info
    paper_names = [f"{p['arxiv_id']}: {p.get('title', 'No title')[:50]}..." for p in papers_data]
    logger.info(f"Processing batch of {len(papers_data)} papers:")
    for name in paper_names:
        logger.info(f"  - {name}")
    
    system_prompt, user_prompt = build_batch_prompt(papers_data)
    
    try:
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=user_prompt,
            config={
                "system_instruction": system_prompt,
                "temperature": 0,
                "response_mime_type": "application/json",
                "response_json_schema": BatchResponse.model_json_schema(),
            }
        )
        
        response_text = response.text
        logger.debug(f"Received raw response ({len(response_text)} chars)")
        
        # Clean the response to fix escape character issues
        cleaned_response = clean_json_response(response_text)
        
        # Try parsing with Pydantic
        try:
            batch_response = BatchResponse.model_validate_json(cleaned_response)
            logger.info(f"✅ Successfully parsed {len(batch_response.papers)} papers from response")
            return batch_response
        except Exception as parse_error:
            logger.warning(f"Pydantic parse failed: {parse_error}")
            
            # Fallback: try manual JSON parsing and construct BatchResponse
            try:
                data = json.loads(cleaned_response)
                batch_response = BatchResponse.model_validate(data)
                logger.info(f"✅ Successfully parsed {len(batch_response.papers)} papers (fallback method)")
                return batch_response
            except Exception as fallback_error:
                logger.error(f"Fallback parse also failed: {fallback_error}")
                logger.debug(f"Cleaned response (first 1000 chars): {cleaned_response[:1000]}")
                return None
        
    except Exception as e:
        error_msg = str(e).lower()
        
        if "quota" in error_msg or "rate" in error_msg or "429" in error_msg:
            if retry_count < MAX_RETRIES:
                logger.warning(f"Rate limit hit. Waiting {RETRY_DELAY}s before retry {retry_count + 1}/{MAX_RETRIES}")
                logger.debug(f"Error details: {e}")
                time.sleep(RETRY_DELAY)
                return query_gemini_batch(papers_data, retry_count + 1)
            else:
                logger.error(f"Max retries exceeded for batch")
                return None
        else:
            logger.error(f"Gemini API Error: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None


def format_paper_for_csv(paper: Paper) -> tuple:
    """Format a single Paper model for CSV output."""
    if not paper:
        return "[]", "[]", "[]", ""

    authors = []
    affiliations = []
    countries = []

    for author in paper.authors:
        authors.append(author.name)
        
        # Clean affiliations
        clean_affs = [aff.replace("\n", " ").strip() for aff in author.affiliations]
        affiliations.append("; ".join(clean_affs))
        
        # Clean countries
        clean_cnts = [cnt.replace("\n", " ").strip() for cnt in author.countries]
        countries.append("; ".join(clean_cnts))

    # First author countries
    first_author_country = "; ".join(
        [cnt.replace("\n", " ").strip() for cnt in paper.first_author_countries]
    )

    return (
        json.dumps(authors),
        json.dumps(affiliations),
        json.dumps(countries),
        first_author_country,
    )


# =======================
# BATCH PROCESSING
# =======================
DOWNLOAD_DELAY_SECONDS = 3  # Delay between downloads (arXiv rate limit)


def collect_batch(df, indices, link_col):
    """
    Collect papers for a batch, downloading and extracting LaTeX.
    Returns list of paper data dicts.
    """
    batch_data = []
    last_download_time = None
    
    for i, index in enumerate(indices):
        row = df.loc[index]
        pdf_link = row[link_col]
        title = row.get('title', 'Unknown')
        
        eprint_url, arxiv_id = get_eprint_url(pdf_link)
        
        if not eprint_url:
            logger.warning(f"[{index}] No valid e-print URL for {arxiv_id or 'unknown'}")
            continue

        # Timing check: ensure we wait at least DOWNLOAD_DELAY_SECONDS between downloads
        if last_download_time is not None:
            elapsed_since_last = time.time() - last_download_time
            if elapsed_since_last < DOWNLOAD_DELAY_SECONDS:
                wait_needed = DOWNLOAD_DELAY_SECONDS - elapsed_since_last
                logger.debug(f"⏱️  Waiting {wait_needed:.2f}s to respect {DOWNLOAD_DELAY_SECONDS}s delay...")
                time.sleep(wait_needed)
        
        download_start = time.time()
        file_path = download_source(eprint_url, arxiv_id)
        download_end = time.time()
        
        if not file_path:
            logger.warning(f"[{index}] Download failed for {arxiv_id}")
            last_download_time = time.time()
            time.sleep(1)
            continue
        
        logger.info(f"⏱️  [{index}] Download took {download_end - download_start:.2f}s for {arxiv_id}")
        
        latex_text = extract_and_read_latex(file_path, arxiv_id)
        
        if latex_text:
            batch_data.append({
                'original_index': index,
                'arxiv_id': arxiv_id,
                'title': title,
                'latex_text': latex_text
            })
            logger.info(f"✅ [{index}] Collected: {title[:50]}...")
        else:
            logger.warning(f"[{index}] No LaTeX text for {arxiv_id}")
        
        # Cleanup after extracting
        cleanup(arxiv_id)
        
        # Record time for next iteration's delay check
        last_download_time = time.time()
        
        # Log timing info
        if i < len(indices) - 1:  # Don't sleep after the last one
            logger.debug(f"⏱️  Starting {DOWNLOAD_DELAY_SECONDS}s delay before next download...")
            time.sleep(DOWNLOAD_DELAY_SECONDS)
            actual_delay = time.time() - last_download_time
            logger.info(f"⏱️  Actual delay: {actual_delay:.2f}s (target: {DOWNLOAD_DELAY_SECONDS}s) ✓" if actual_delay >= DOWNLOAD_DELAY_SECONDS - 0.1 else f"⚠️  Delay was {actual_delay:.2f}s, under target!")
    
    logger.info(f"📦 Batch collection complete: {len(batch_data)}/{len(indices)} papers collected")
    return batch_data


def process_batch_results(batch_response: BatchResponse, batch_data: list, output_csv: str) -> int:
    """
    Process and save batch results to CSV and JSON.
    batch_response: Pydantic BatchResponse model
    batch_data: list of dicts with original_index, arxiv_id, title, latex_text
    """
    if not batch_response or not batch_response.papers:
        logger.error("Invalid or empty batch response")
        return 0
    
    saved_count = 0
    papers_map = {p['arxiv_id']: p for p in batch_data}
    
    for paper in batch_response.papers:
        arxiv_id = paper.arxiv_id
        
        # Find matching paper data
        original_data = papers_map.get(arxiv_id)
        if not original_data:
            # Try to match by position if ID doesn't match exactly
            logger.warning(f"Could not match arxiv_id: {arxiv_id}")
            continue
        
        original_index = original_data['original_index']
        
        # Log to JSON (serialize Pydantic model to dict)
        with open(OUTPUT_JSON, "a", encoding='utf-8') as f:
            entry = {
                "index": original_index,
                "arxiv_id": arxiv_id,
                "data": paper.model_dump()
            }
            f.write(json.dumps(entry) + "\n")
        
        # Format for CSV
        authors, affils, countries, first_country = format_paper_for_csv(paper)
        
        # Append to CSV
        res_df = pd.DataFrame([{
            "original_index": original_index,
            "arxiv_id": arxiv_id,
            "extracted_authors": authors,
            "extracted_affiliations": affils,
            "extracted_countries": countries,
            "first_author_country": first_country,
        }])
        res_df.to_csv(output_csv, mode="a", header=False, index=False)
        
        logger.info(f"[{original_index}] Saved result for {arxiv_id}")
        saved_count += 1
    
    return saved_count


def main():
    logger.info("=" * 60)
    logger.info("GEMINI AUTHOR AFFILIATION EXTRACTOR")
    logger.info(f"Model: {MODEL_ID}")
    logger.info(f"Batch Size: {BATCH_SIZE}")
    logger.info(f"Max LaTeX chars per paper: {MAX_LATEX_CHARS}")
    logger.info("=" * 60)
    
    setup_directories()
    
    # Read dataset
    logger.info(f"Reading dataset: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    logger.info(f"Total papers in dataset: {len(df)}")
    
    # Find link column
    link_col = None
    for col in df.columns:
        if "pdf_link" in col.lower():
            link_col = col
            break
    
    if not link_col:
        for col in df.columns:
            if len(df) > 0:
                val = str(df[col].iloc[0])
                if "arxiv.org" in val:
                    link_col = col
                    break
    
    if not link_col:
        logger.error("Could not find pdf_link column")
        return
    
    logger.info(f"Using link column: {link_col}")
    
    # Ensure output CSV has header if new
    if not os.path.exists(OUTPUT_CSV_PATH):
        pd.DataFrame(columns=[
            "original_index",
            "arxiv_id",
            "extracted_authors",
            "extracted_affiliations",
            "extracted_countries",
            "first_author_country",
        ]).to_csv(OUTPUT_CSV_PATH, index=False)
        logger.info(f"Created output CSV: {OUTPUT_CSV_PATH}")
    
    # Get already processed indices
    processed_indices = set()
    try:
        existing = pd.read_csv(OUTPUT_CSV_PATH)
        if "original_index" in existing.columns:
            processed_indices = set(existing["original_index"].tolist())
            logger.info(f"Found {len(processed_indices)} already processed entries")
    except:
        pass
    
    # Filter to unprocessed entries
    indices_to_process = [i for i in df.index if i not in processed_indices]
    
    if not indices_to_process:
        logger.info("No new entries to process.")
        return
    
    logger.info(f"Papers to process: {len(indices_to_process)}")
    
    # Process in batches
    total_saved = 0
    total_batches = (len(indices_to_process) + BATCH_SIZE - 1) // BATCH_SIZE
    
    pbar = tqdm(
        range(0, len(indices_to_process), BATCH_SIZE),
        total=total_batches,
        desc="Processing batches"
    )
    
    for batch_start in pbar:
        batch_end = min(batch_start + BATCH_SIZE, len(indices_to_process))
        batch_indices = indices_to_process[batch_start:batch_end]
        
        logger.info(f"\n{'='*40}")
        logger.info(f"BATCH {batch_start // BATCH_SIZE + 1}/{total_batches}")
        logger.info(f"Indices: {batch_indices}")
        logger.info(f"{'='*40}")
        
        # Print paper titles for sanity check
        print("\n" + "=" * 50)
        print(f"📚 BATCH {batch_start // BATCH_SIZE + 1}/{total_batches} - Papers:")
        for idx in batch_indices:
            title = df.loc[idx].get('title', 'Unknown')[:60]
            print(f"   [{idx}] {title}...")
        print("=" * 50 + "\n")
        
        # Collect batch data (download and extract LaTeX)
        start_time = time.time()
        batch_data = collect_batch(df, batch_indices, link_col)
        
        if not batch_data:
            logger.warning("No valid papers in this batch, skipping...")
            continue
        
        # Query Gemini with batch
        logger.info(f"Sending batch of {len(batch_data)} papers to Gemini...")
        batch_response = query_gemini_batch(batch_data)
        
        if batch_response:
            saved = process_batch_results(batch_response, batch_data, OUTPUT_CSV_PATH)
            total_saved += saved
            logger.info(f"Batch complete: {saved}/{len(batch_data)} papers saved")
            pbar.set_postfix({"saved": total_saved})
        else:
            logger.error("Failed to get response for batch")
        
        # Rate limiting
        elapsed = time.time() - start_time
        wait_time = max(0, RATE_LIMIT_SECONDS - elapsed)
        if wait_time > 0:
            logger.debug(f"Rate limit: waiting {wait_time:.1f}s")
            time.sleep(wait_time)
    
    logger.info("=" * 60)
    logger.info(f"PROCESSING COMPLETE")
    logger.info(f"Total papers saved: {total_saved}")
    logger.info(f"Output CSV: {OUTPUT_CSV_PATH}")
    logger.info(f"Output JSON: {OUTPUT_JSON}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
