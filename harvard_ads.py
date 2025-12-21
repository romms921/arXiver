import pandas as pd
import requests
import time
import os
import re
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

# --- CONFIGURATION ---
# Ensure ADS_API_KEY is set in your environment or .env file
# To get a token: https://ui.adsabs.harvard.edu/user/settings/token
ADS_API_KEY = os.getenv("ADS_API_KEY", "")
ADS_URL = "https://api.adsabs.harvard.edu/v1/search/query"

CSV_PATH = "FINAL_ARXIV_2025_copy_updated.csv"
OUTPUT_PATH = "papers_with_ads_metrics.csv"

# Batching and Throttling
BATCH_SIZE = 10
# 1000 requests/hour is ~0.27 requests/second. 
# To be safe, we'll aim for ~800/hour (~0.22/s)
# Each batch takes 1 (lookup) + up to BATCH_SIZE (citations) requests.
# If batch lookup + citations take X requests, we sleep to ensure we don't exceed rate.
RETRY_WAIT = 60 
MAX_CITATIONS_TO_CHECK = 1000 # ADS search rows limit (max 2000)

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
            # 3600s / 1000 req = 3.6s per request. 
            # If we want 1000/hr, we just ensure we don't go faster than 1 req / 3.6s.
            time.sleep(3.6) 
            
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
            "fl": "bibcode,title,author,aff,citation_count,identifier",
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

def get_non_self_citations(bibcode, target_authors):
    """Fetches citation details and filters out self-citations."""
    params = {
        "q": f'citations("{bibcode}")',
        "fl": "author",
        "rows": MAX_CITATIONS_TO_CHECK
    }
    data = ads_request(params)
    if not data: return None
        
    docs = data.get("response", {}).get("docs", [])
    if not docs: return 0
        
    t_auths = {a.lower().strip() for a in target_authors if isinstance(a, str)}
    
    non_self = 0
    for doc in docs:
        c_auths = {a.lower().strip() for a in doc.get("author", []) if isinstance(a, str)}
        if not (t_auths & c_auths):
            non_self += 1
            
    return non_self

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
            params = {"q": f'title:"{clean_title}"', "fl": "bibcode,author,aff,citation_count", "rows": 1}
            data = ads_request(params)
            if data and data.get("response", {}).get("docs"):
                doc = data["response"]["docs"][0]
                
    if not doc:
        return {
            "title": title, "ads_bibcode": "N/A", "affiliations": "N/A",
            "total_citations": 0, "non_self_citations": 0, "status": "not_found"
        }

    bibcode = doc.get("bibcode")
    total = doc.get("citation_count", 0)
    authors = doc.get("author", [])
    affs = doc.get("aff", [])
    
    # Format Affiliations
    # Zip authors and affiliations if available, else just unique institutions
    aff_list = []
    if len(authors) == len(affs):
        for auth, aff in zip(authors, affs):
            if aff and aff != "-":
                aff_list.append(f"{auth}: {aff}")
    else:
        # Fallback to unique institutions
        aff_list = list(dict.fromkeys([a for a in affs if a and a != "-"]))
    
    aff_str = "; ".join(aff_list)
    
    non_self = total
    if 0 < total <= MAX_CITATIONS_TO_CHECK:
        ns = get_non_self_citations(bibcode, authors)
        if ns is not None: non_self = ns
            
    return {
        "title": title, "ads_bibcode": bibcode, "affiliations": aff_str,
        "total_citations": total, "non_self_citations": non_self, "status": "success"
    }

def main():
    if not os.path.exists(CSV_PATH):
        print(f"File not found: {CSV_PATH}")
        return

    # Load data
    df = pd.read_csv(CSV_PATH)
    
    # Progress tracking
    if os.path.exists(OUTPUT_PATH):
        try:
            p_df = pd.read_csv(OUTPUT_PATH)
            p_titles = set(p_df['title'].tolist())
        except:
            p_titles = set()
    else:
        p_titles = set()

    todo = df[~df['title'].isin(p_titles)].copy()
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

    print("Success! Data enriched with ADS metrics.")

if __name__ == "__main__":
    main()
