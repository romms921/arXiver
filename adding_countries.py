import pandas as pd
import requests
import tarfile
import os
import shutil
import re
import time
import threading
import queue
from tqdm import tqdm

# ==========================================
# CONFIGURATION
# ==========================================

INPUT_FILE = '2025_Data.csv'
COORDS_FILE = 'world_coords.csv'
OUTPUT_FILE = '2025_Data_processed_1.csv'
TEMP_DIR = 'temp_arxiv_source'
SAVE_INTERVAL = 10
ARXIV_RATE_LIMIT = 3  # Seconds (strict)
BUFFER_LINES = 30     # Stop reading if no new countries found for this many lines

# Thread-safe Queue
work_queue = queue.Queue()

# ==========================================
# DATA LOADING & REGEX PREP
# ==========================================

replacement_dict = {
    'USA': 'United States',
    'UK': 'United Kingdom',
    'UAE': 'United Arab Emirates',
    'United States of America': 'United States',
    "People's Republic of China": 'China',
    'The Netherlands': 'Netherlands',
    'Republic of Korea': 'South Korea',
    'The United Kingdom': 'United Kingdom'
}

if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

print("Loading datasets...")
try:
    df = pd.read_csv(INPUT_FILE)
    # Only first 10000 rows
    df = df.head(10000)

    world_df = pd.read_csv(COORDS_FILE)
except FileNotFoundError as e:
    print(f"Error: {e}")
    exit()

if 'latex_countries' not in df.columns:
    df['latex_countries'] = None

# Build Regex
base_countries = set(world_df['country'].dropna().unique())
search_map = {country: country for country in base_countries}
search_map.update(replacement_dict)

# Sort by length (descending) to match "United States" before "United"
search_terms = sorted(search_map.keys(), key=len, reverse=True)
pattern_str = r'\b(' + '|'.join(map(re.escape, search_terms)) + r')\b'
country_regex = re.compile(pattern_str, re.IGNORECASE) 

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def get_arxiv_id(url):
    match = re.search(r'(\d+\.\d+)', str(url))
    return match.group(1) if match else None

def get_all_latex_lines(directory):
    """
    Reads all .tex files and returns a single list of strings (lines).
    """
    all_lines = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.tex'):
                path = os.path.join(root, file)
                try:
                    with open(path, 'r', encoding='utf-8', errors='replace') as f:
                        all_lines.extend(f.readlines())
                except:
                    continue
    return all_lines

def process_lines_with_buffer(lines):
    """
    Scans lines for countries. 
    Stops if 30 lines pass after a match without finding a new match.
    STORES DUPLICATES (e.g. Germany, Germany).
    """
    found_matches = []  # CHANGED: List instead of Set to allow duplicates
    lines_since_last_match = 0
    has_found_any = False
    
    for line in lines:
        # Quick regex search on single line
        matches = country_regex.findall(line)
        
        if matches:
            # Match found in this line
            has_found_any = True
            lines_since_last_match = 0 # Reset buffer
            
            for match in matches:
                # Resolve to standard name
                for key in search_map:
                    if key.lower() == match.lower():
                        # CHANGED: Append directly to keep duplicates
                        found_matches.append(search_map[key])
                        break
        else:
            # No match in this line
            if has_found_any:
                lines_since_last_match += 1
                
                # CHECK BUFFER LIMIT
                if lines_since_last_match >= BUFFER_LINES:
                    # We assume the affiliation section is over
                    break
                    
    # CHANGED: Return the list directly (preserves order of appearance)
    return found_matches

# ==========================================
# THREAD 1: DOWNLOADER (PRODUCER)
# ==========================================

def downloader_thread(df_to_process, pbar):
    headers = {'User-Agent': 'Mozilla/5.0 (DataProcessingScript/1.0)'}

    for index, row in df_to_process.iterrows():
        # Skip if already done
        if pd.notna(row['latex_countries']) and row['latex_countries'] != "":
            pbar.update(1)
            continue

        pdf_link = row['pdf_link']
        arxiv_id = get_arxiv_id(pdf_link)
        
        if not arxiv_id:
            pbar.update(1)
            continue

        pbar.set_description(f"Downloading {arxiv_id}")
        
        paper_dir = os.path.join(TEMP_DIR, arxiv_id)
        if not os.path.exists(paper_dir):
            os.makedirs(paper_dir)

        url = f"https://arxiv.org/e-print/{arxiv_id}"
        download_success = False

        try:
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            if response.status_code == 200:
                temp_filepath = os.path.join(paper_dir, 'source_file')
                with open(temp_filepath, 'wb') as f:
                    f.write(response.content)
                download_success = True
        except Exception:
            pass

        if download_success:
            work_queue.put((index, arxiv_id, paper_dir))
        else:
            try: shutil.rmtree(paper_dir) 
            except: pass
            pbar.update(1)

        # Strict Sleep for ArXiv Rate Limit
        time.sleep(ARXIV_RATE_LIMIT)

    work_queue.put(None) # Sentinel

# ==========================================
# THREAD 2: PROCESSOR (CONSUMER)
# ==========================================

def processor_thread(pbar):
    processed_count = 0
    
    while True:
        item = work_queue.get()
        if item is None: break
            
        index, arxiv_id, paper_dir = item
        source_file = os.path.join(paper_dir, 'source_file')
        
        pbar.set_description(f"Processing {arxiv_id}")

        # 1. Extract
        extraction_success = False
        try:
            if tarfile.is_tarfile(source_file):
                with tarfile.open(source_file) as tar:
                    tar.extractall(path=paper_dir)
                extraction_success = True
            else:
                # Try Gzip
                import gzip
                with gzip.open(source_file, 'rb') as f_in:
                    content = f_in.read()
                    if b'\\documentclass' in content or b'\\begin' in content:
                        with open(os.path.join(paper_dir, 'main.tex'), 'wb') as f_out:
                            f_out.write(content)
                        extraction_success = True
        except:
            pass

        # 2. Analyze (With Buffer Logic)
        countries_found = []
        if extraction_success:
            # Get all lines as a list
            all_lines = get_all_latex_lines(paper_dir)
            
            # Run the smart buffer search
            countries_found = process_lines_with_buffer(all_lines)
            
            if countries_found:
                # Only print first few to keep log clean, but process all
                display_str = ', '.join(countries_found[:5])
                if len(countries_found) > 5: display_str += "..."
                tqdm.write(f"   [{arxiv_id}] Found: {display_str}")

        # 3. Update & Save
        df.at[index, 'latex_countries'] = ", ".join(countries_found)
        
        processed_count += 1
        if processed_count % SAVE_INTERVAL == 0:
            df.to_csv(OUTPUT_FILE, index=False)

        # 4. Cleanup
        try: shutil.rmtree(paper_dir)
        except: pass
        
        pbar.update(1)
        work_queue.task_done()

# ==========================================
# MAIN EXECUTION
# ==========================================

print("Starting Optimized Processing (Buffer Stop Mode - Allowing Duplicates)...")

total_papers = df.shape[0]
pbar = tqdm(total=total_papers, unit="paper")

t_consumer = threading.Thread(target=processor_thread, args=(pbar,))
t_consumer.start()

t_producer = threading.Thread(target=downloader_thread, args=(df, pbar,))
t_producer.start()

t_producer.join()
t_consumer.join()

pbar.close()

df.to_csv(OUTPUT_FILE, index=False)
try: shutil.rmtree(TEMP_DIR)
except: pass
print("Done.")