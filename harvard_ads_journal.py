import pandas as pd
import requests
import time
import os
import re
import dotenv
from tqdm import tqdm

dotenv.load_dotenv()

# --- CONFIGURATION ---
# Ensure ADS_API_KEY is set in your environment or .env file
ADS_API_KEY = os.getenv("ADS_API_KEY")
ADS_URL = "https://api.adsabs.harvard.edu/v1/search/query"

CSV_PATH = "FINAL_ARXIV_2025_Process.csv"
OUTPUT_PATH = "papers_with_journals_3.csv"

# Index Range (corresponds to row numbers in CSV)
START_INDEX = 18334
STOP_INDEX = 18661  # Set to None to process until the end

# Batching and Throttling
BATCH_SIZE = 10
RETRY_WAIT = 60

def get_headers():
    return {"Authorization": f"Bearer {ADS_API_KEY}"}

def extract_arxiv_id(pdf_link):
    if not isinstance(pdf_link, str): return None
    m = re.search(r'arxiv\.org/(?:pdf|abs)/([\d\.]+)', pdf_link)
    return m.group(1) if m else None

def ads_request(params):
    """Execution wrapper for ADS API calls with rate limit handling."""
    if not ADS_API_KEY:
        raise ValueError("ADS_API_KEY not found in environment.")
        
    while True:
        try:
            r = requests.get(ADS_URL, headers=get_headers(), params=params, timeout=20)
            
            # Rate limit check (429 Too Many Requests)
            if r.status_code == 429:
                reset = r.headers.get('X-RateLimit-Reset', '60')
                print(f"\n[Rate Limit] Sleeping for {RETRY_WAIT}s (Reset info: {reset})")
                time.sleep(RETRY_WAIT)
                continue
                
            r.raise_for_status()
            
            # Optional: Short sleep to stay under the 1000/hr limit
            time.sleep(3.7) 
            
            return r.json()
        except requests.exceptions.HTTPError as e:
            if r.status_code == 401:
                print("\n[Error] 401: Unauthorized. Is your ADS_API_KEY correct?")
                return None
            print(f"\n[HTTP Error] {e}")
            return None
        except Exception as e:
            print(f"\n[Request Error] {e}")
            return None

def resolve_papers_ads(batch_df):
    """Resolves a list of papers by ArXiv ID (batch) or fallback to Title."""
    results_map = {}
    
    # 1. Batch ArXiv Lookup
    ids = [extract_arxiv_id(row.get('pdf_link')) for _, row in batch_df.iterrows()]
    valid_ids = [aid for aid in ids if aid]
    
    if valid_ids:
        q = " OR ".join([f'identifier:"arXiv:{aid}"' for aid in valid_ids])
        params = {
            "q": q,
            "fl": "bibcode,title,pub,identifier",
            "rows": len(valid_ids) * 2
        }
        data = ads_request(params)
        if data:
            for doc in data.get("response", {}).get("docs", []):
                for iden in doc.get('identifier', []):
                    if iden.startswith('arXiv:'):
                        results_map[f"arxiv:{iden.split(':')[-1]}"] = doc
                        break
    return results_map

def process_paper(row, ads_batch):
    """Processes a single row, resolving via batch results or title fallback."""
    title = row.get('title', '')
    arxiv_id = extract_arxiv_id(row.get('pdf_link'))
    
    doc = None
    if arxiv_id and f"arxiv:{arxiv_id}" in ads_batch:
        doc = ads_batch[f"arxiv:{arxiv_id}"]
    
    # Fallback: Title search
    if not doc:
        clean_title = re.sub(r'[^\w\s]', ' ', title).strip()
        if clean_title:
            params = {"q": f'title:"{clean_title}"', "fl": "bibcode,pub", "rows": 1}
            data = ads_request(params)
            if data and data.get("response", {}).get("docs"):
                doc = data["response"]["docs"][0]
                
    if not doc:
        return {
            "title": title, "abs_journal": "N/A", "status": "not_found"
        }

    journal = doc.get("pub", "N/A")
            
    return {
        "title": title, "abs_journal": journal, "status": "success"
    }

def main():
    if not os.path.exists(CSV_PATH):
        print(f"File not found: {CSV_PATH}")
        return

    # Load data
    df = pd.read_csv(CSV_PATH)
    
    # Slice dataframe by index range
    if STOP_INDEX is not None:
        df = df.iloc[START_INDEX:STOP_INDEX]
    else:
        df = df.iloc[START_INDEX:]
        
    if df.empty:
        print(f"No papers in range {START_INDEX} to {STOP_INDEX}")
        return

    # Filter for mismatch: journal_flag == 0 AND total_citations > 0
    if 'journal_flag' not in df.columns or 'total_citations' not in df.columns:
        print("Error: 'journal_flag' or 'total_citations' columns missing.")
        return

    todo = df[(df['journal_flag'] == 0) & (df['total_citations'] > 0)].copy()

    if todo.empty:
        print("No papers with mismatch (journal_flag=0 and citations>0) found.")
        return

    # Progress tracking
    if os.path.exists(OUTPUT_PATH):
        try:
            p_df = pd.read_csv(OUTPUT_PATH)
            p_titles = set(p_df['title'].tolist())
        except:
            p_titles = set()
    else:
        p_titles = set()

    todo = todo[~todo['title'].isin(p_titles)]
    if todo.empty:
        print("Everything is up to date.")
        return

    print(f"Processing {len(todo)} papers. Target: {OUTPUT_PATH}")
    
    current_results = []
    for i in tqdm(range(0, len(todo), BATCH_SIZE)):
        batch = todo.iloc[i : i + BATCH_SIZE]
        
        # Resolve batch
        batch_mapping = resolve_papers_ads(batch)
        
        for _, row in batch.iterrows():
            res = process_paper(row, batch_mapping)
            current_results.append(res)
            
        # Periodic Save
        if len(current_results) >= 20: 
            save_df = pd.DataFrame(current_results)
            header = not os.path.exists(OUTPUT_PATH)
            save_df.to_csv(OUTPUT_PATH, mode='a', header=header, index=False)
            current_results = []

    # Final dump
    if current_results:
        save_df = pd.DataFrame(current_results)
        header = not os.path.exists(OUTPUT_PATH)
        save_df.to_csv(OUTPUT_PATH, mode='a', header=header, index=False)

    print("Success! Data enriched with ADS journals.")

if __name__ == "__main__":
    main()