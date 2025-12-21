import pandas as pd
import requests
import time
import re
import json
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

# --- CONFIGURATION ---
EMAIL = "rommuluslewis@gmail.com"  # Replace with yours to enter the OpenAlex 'Polite Pool'
OPENALEX_API = "https://api.openalex.org"
MAX_WORKERS = 10                  # Number of parallel requests (8-12 is usually safe)
MAX_CITATIONS_TO_CHECK = 500      # Skip manual self-cite check for papers with >500 citations
CSV_PATH = "FINAL_ARXIV_2025_copy_updated.csv"
OUTPUT_PATH = "papers_with_metrics.csv"

HEADERS = {
    "User-Agent": f"AstroResearchBot/1.0 (mailto:{EMAIL})"
}

def extract_arxiv_id(pdf_link):
    if not isinstance(pdf_link, str): return None
    m = re.search(r'arxiv\.org/pdf/([\d\.]+)', pdf_link)
    return m.group(1) if m else None

def get_openalex_data(arxiv_id, title):
    """Fetches a work from OpenAlex by ArXiv ID or Title."""
    try:
        # Strategy 1: ArXiv ID (Most Accurate)
        if arxiv_id:
            url = f"{OPENALEX_API}/works/https://arxiv.org/abs/{arxiv_id}"
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                return r.json()

        # Strategy 2: Title Search
        clean_title = quote(title.strip())
        url = f"{OPENALEX_API}/works?filter=title.search:{clean_title}&per-page=1"
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            res = r.json()
            if res.get("results"):
                return res["results"][0]
    except Exception:
        pass
    return None

def process_paper(row):
    """Main processing logic for a single row."""
    title = row.get('title', '')
    pdf_link = row.get('pdf_link', '')
    arxiv_id = extract_arxiv_id(pdf_link)
    
    work = get_openalex_data(arxiv_id, title)
    
    if not work:
        return {
            "title": title,
            "affiliations": "Not Found",
            "total_citations": 0,
            "non_self_citations": 0,
            "status": "not_found"
        }

    # 1. Author Affiliations
    affs = set()
    for authorship in work.get("authorships", []):
        for inst in authorship.get("institutions", []):
            affs.add(inst.get("display_name"))
    
    # 2. Total Citations
    total_cites = work.get("cited_by_count", 0)
    
    # 3. Non-Self Citations (Calculated)
    non_self_cites = total_cites # Default to total
    author_ids = {a.get("author", {}).get("id") for a in work.get("authorships", []) if a.get("author")}
    
    if 0 < total_cites <= MAX_CITATIONS_TO_CHECK:
        try:
            # Fetch everyone who cited this paper
            cites_url = f"{OPENALEX_API}/works?filter=cites:{work['id']}&per-page=200"
            c_res = requests.get(c_res_url, headers=HEADERS).json()
            
            actual_non_self = 0
            for citing_work in c_res.get("results", []):
                citing_authors = {a.get("author", {}).get("id") for a in citing_work.get("authorships", [])}
                # If no overlap in author IDs, it's not a self-citation
                if not (author_ids & citing_authors):
                    actual_non_self += 1
            non_self_cites = actual_non_self
        except:
            pass # Keep default if fetch fails

    return {
        "title": title,
        "affiliations": "; ".join(filter(None, affs)),
        "total_citations": total_cites,
        "non_self_citations": non_self_cites,
        "status": "success"
    }

def main():
    data = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(data)} papers. Starting enrichment...")

    results = []
    # Using ThreadPoolExecutor for speed
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_paper, row): i for i, row in data.iterrows()}
        
        # Loading bar
        for future in tqdm(as_completed(futures), total=len(futures), desc="Enriching Data"):
            results.append(future.result())

    # Merge results back to original dataframe
    res_df = pd.DataFrame(results)
    final_df = data.merge(res_df, on='title', how='left')
    final_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Done! Results saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()