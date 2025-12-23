import pandas as pd
import requests
import tarfile
import io
import re
import time
import ast
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


# ========================
# CONFIG
# ========================

CSV_PATH = "test.csv"
OUTPUT_PATH = "latex_affiliations_output_2.txt"
SLEEP_SECONDS = 0.5  # Reduced sleep for parallel threads
MAX_WORKERS = 5      # arXiv is strict; keep this low


# ========================
# GLOBALS FOR TRACKING
# ========================

write_lock = threading.Lock()
success_count = 0
fail_count = 0


# ========================
# UTILITIES
# ========================

def extract_arxiv_id(pdf_link):
    """Extract arXiv ID from PDF link."""
    if not isinstance(pdf_link, str):
        return None
    m = re.search(r'arxiv\.org/(pdf|abs)/([0-9.]+)', pdf_link)
    return m.group(2) if m else None


def affiliations_need_fix(val):
    """
    Returns True if affiliations is a list and contains None or needs fixing.
    This is the same logic as in remaining_affil_from_pdf.py
    """
    if pd.isna(val):
        return True

    if isinstance(val, str):
        try:
            val = ast.literal_eval(val)
        except Exception:
            return True

    if isinstance(val, list):
        return any(v is None or str(v).strip().lower() == "none" for v in val)

    return True


def download_tex_sources(arxiv_id):
    """
    Download LaTeX source files from arXiv.
    Returns a dictionary of filename -> content.
    """
    try:
        url = f"https://arxiv.org/e-print/{arxiv_id}"
        r = requests.get(url, timeout=10) # 5 might be too aggressive for larger files
        if r.status_code != 200:
            return None

        fileobj = io.BytesIO(r.content)

        # Try gzip, fallback to plain tar
        try:
            tar = tarfile.open(fileobj=fileobj, mode="r:gz")
        except tarfile.ReadError:
            tar = tarfile.open(fileobj=fileobj, mode="r:")

        tex_files = {}
        for member in tar.getmembers():
            if member.name.endswith(".tex"):
                tex_files[member.name] = (
                    tar.extractfile(member)
                    .read()
                    .decode(errors="ignore")
                )

        return tex_files
    except Exception as e:
        # Silencing error output for parallel execution to keep console clean
        return None


def find_affiliation_section(tex_content):
    """
    Search for author affiliation section in LaTeX file.
    
    Strategy: Search for keywords like University, Institute, Department, etc.
    When found, extract 50 lines before and 50 lines after the first match.
    
    Returns the relevant section containing affiliations.
    """
    # Remove comments
    tex_nc = re.sub(r"%.*", "", tex_content)
    
    # Split into lines for line-based extraction
    lines = tex_nc.split('\n')
    
    # Keywords to search for (case-insensitive)
    keywords = [
        'University', 'Institute', 'Department', 'Centre', 'Center',
        'College', 'Laboratory', 'Observatory', 'School of',
        '\\\\affiliation', '\\\\affil', '\\\\address', '\\\\institute'
    ]
    
    # Find the first line that contains any keyword
    first_match_line = -1
    for i, line in enumerate(lines):
        for keyword in keywords:
            if keyword.lower() in line.lower():
                first_match_line = i
                break
        if first_match_line >= 0:
            break
    
    if first_match_line < 0:
        return None
    
    # Extract 50 lines before and 50 lines after
    start_line = max(0, first_match_line - 50)
    end_line = min(len(lines), first_match_line + 50)
    
    # Join the extracted lines
    extracted_section = '\n'.join(lines[start_line:end_line])
    
    return extracted_section


# ========================
# WORKER LOGIC
# ========================

def process_row_worker(index, row, total_to_process):
    """
    Worker function to process a single paper row.
    Handles I/O and thread-safe writing.
    """
    global success_count, fail_count
    
    arxiv_id = extract_arxiv_id(row.get("pdf_link", ""))
    title = row.get("title", "Unknown Title")
    
    result_text = ""
    status_msg = ""
    is_success = False
    
    if not arxiv_id:
        status_msg = f"✗ Failed: No arXiv ID found"
    else:
        tex_files = download_tex_sources(arxiv_id)
        if not tex_files:
            status_msg = f"✗ Failed: Failed to download LaTeX sources"
        else:
            # First, try files that have \author or \affil commands
            priority_files = []
            other_files = []
            
            for filename, content in tex_files.items():
                if any(cmd in content for cmd in ['\\author', '\\affil', '\\address']):
                    priority_files.append((filename, content))
                else:
                    other_files.append((filename, content))
            
            affil_section = None
            for filename, content in priority_files + other_files:
                affil_section = find_affiliation_section(content)
                if affil_section:
                    break
            
            if affil_section:
                result_text = f"{'-'*80}\nPAPER: {title}\n{'-'*80}\nAFFILIATION SECTION:\n{affil_section}\n\n"
                status_msg = f"✓ Successfully extracted affiliations"
                is_success = True
            else:
                status_msg = f"✗ Failed: No affiliation section found"
                result_text = f"{'-'*80}\nPAPER: {title}\n{'-'*80}\nERROR: No affiliation section found\n\n"

    # Thread-safe write and counter update
    with write_lock:
        if is_success:
            success_count += 1
        else:
            fail_count += 1
            if not result_text:
                result_text = f"{'-'*80}\nPAPER: {title}\n{'-'*80}\nERROR: {status_msg.replace('✗ Failed: ', '')}\n\n"
        
        with open(OUTPUT_PATH, 'a', encoding='utf-8') as outfile:
            outfile.write(result_text)
        
        # Batch log to console
        print(f"[{success_count + fail_count}/{total_to_process}] {title[:60]}... -> {status_msg}")

def main():
    """
    Main function to process papers from CSV and extract affiliations in parallel.
    """
    print(f"Reading CSV from: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    
    # Only beyond a certain index for testing
    df = df.iloc[10390:]  # Change 0 to desired starting index if needed

    # Identify which papers to process
    to_process_indices = df[df["affiliations"].apply(affiliations_need_fix)].index.tolist()
    total = len(to_process_indices)
    print(f"Total papers needing affiliation fix: {total}")
    
    # Initialize/Clear output file
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as outfile:
        outfile.write("=" * 80 + "\n")
        outfile.write("LaTeX Affiliation Extraction Results\n")
        outfile.write("=" * 80 + "\n\n")
        
    print(f"Starting parallel extraction with {MAX_WORKERS} workers...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for i in to_process_indices:
            row = df.loc[i]
            futures.append(executor.submit(process_row_worker, i, row, total))
            # Slightly stagger thread starts to avoid instant burst to arXiv
            time.sleep(0.1)
        
        # Wait for all to complete
        for future in as_completed(futures):
            future.result() # To raise any exceptions caught in threads
    
    # Write summary at the end
    with open(OUTPUT_PATH, 'a', encoding='utf-8') as outfile:
        outfile.write("=" * 80 + "\n")
        outfile.write("SUMMARY\n")
        outfile.write("=" * 80 + "\n")
        outfile.write(f"Total processed: {success_count + fail_count}\n")
        outfile.write(f"Successful extractions: {success_count}\n")
        outfile.write(f"Failed extractions: {fail_count}\n")
    
    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"{'='*60}")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Output written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
