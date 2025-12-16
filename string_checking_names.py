import pandas as pd
import requests
import bs4
import time
import re
import os

# Load existing data
print("Loading existing papers from arxiv_papers_copy.csv...")
try:
    data = pd.read_csv("arxiv_papers_copy.csv")
    
    # helper to extract ID from arxiv.org/pdf/2501.00089
    def extract_id_from_url(url):
        if pd.isna(url): return ""
        # Get filename part
        part = str(url).split('/')[-1]
        # Match standard arxiv ID pattern (digits.digits) or old pattern (chem-ph/xxxx)
        # Focus on 2025 papers which are usually YYMM.NNNNN
        # remove version 'v1', 'v2' etc if attached
        # e.g. 2501.00089v1 -> 2501.00089
        # e.g. 2501.00089 -> 2501.00089
        match = re.match(r'(\d{4}\.\d{4,5})', part)
        if match:
            return match.group(1)
        return part # fallback

    existing_ids = set(data['pdf_link'].apply(extract_id_from_url))
    print(f"Loaded {len(existing_ids)} unique existing IDs.")

except FileNotFoundError:
    print("Error: arxiv_papers_copy.csv not found.")
    exit()

missing_papers = []
stats = []

# Loop for all 12 months of 2025
months = range(1, 13)
base_url = "https://arxiv.org/list/astro-ph/2025-{:02d}?skip={}&show=2000"

print("Starting to check months using ID matching...")

for month in months:
    month_str = f"2025-{month:02d}"
    print(f"Processing {month_str}...")
    month_missing_count = 0
    skip = 0
    
    while True:
        url = base_url.format(month, skip)
        print(f"  Fetching {url}...")
        try:
            response = requests.get(url)
            if response.status_code != 200:
                print(f"  Failed to fetch {url}: Status {response.status_code}")
                break
            
            soup = bs4.BeautifulSoup(response.content, 'html.parser')
            
            # ArXiv list pages use <dl> <dt>...</dt> <dd>...</dd> </dl>
            # <dt> contains the identifier (arXiv:2501.XXXX)
            # <dd> contains the title
            dts = soup.find_all('dt')
            dds = soup.find_all('dd')
            
            if not dts:
                if skip == 0:
                    print(f"  No papers found for {month_str} (or page structure changed).")
                break
            
            print(f"  Found {len(dts)} papers on this page.")
            
            # Iterate over pairs
            for dt, dd in zip(dts, dds):
                # Extract ID
                # <a href="/abs/2501.00089" title="Abstract">arXiv:2501.00089</a>
                anchor = dt.find('a', title='Abstract')
                if not anchor:
                    continue
                
                id_text = anchor.get_text().strip() # "arXiv:2501.00089"
                if id_text.startswith("arXiv:"):
                    clean_id = id_text[6:].strip()
                else:
                    clean_id = id_text.strip()
                
                # Compare with existing
                if clean_id not in existing_ids:
                    # Extract Title
                    # <div class="list-title mathjax">Title: ...</div>
                    title_div = dd.find('div', class_='list-title mathjax')
                    if title_div:
                        raw_title = title_div.get_text().strip()
                        if raw_title.startswith("Title:"):
                            title_text = raw_title[6:].strip()
                        else:
                            title_text = raw_title
                    else:
                        title_text = "Unknown Title"
                        
                    missing_papers.append({'title': title_text, 'month': month_str, 'id': clean_id})
                    month_missing_count += 1
            
            # Pagination check
            if len(dts) >= 2000:
                skip += 2000
                time.sleep(1) # Polite delay
            else:
                break
                
        except Exception as e:
            print(f"  Error processing {month_str}: {e}")
            break
    
    stats.append({'month': month_str, 'Missing_Count': month_missing_count})
    print(f"  Missing papers count for {month_str}: {month_missing_count}")
    time.sleep(1) # Polite delay between months

# Save results
missing_csv = "months_missing_papers.csv"
stats_csv = "missing_papers_stats.csv"

if missing_papers:
    df_missing = pd.DataFrame(missing_papers)
    df_missing.to_csv(missing_csv, index=False)
    print(f"\nSaved {len(df_missing)} missing papers to {missing_csv}")
else:
    print(f"\nNo missing papers found.")
    pd.DataFrame(columns=['title', 'month', 'id']).to_csv(missing_csv, index=False)

# Save Stats
df_stats = pd.DataFrame(stats)
df_stats.to_csv(stats_csv, index=False)
print(f"Saved stats to {stats_csv}")

print("\n--- Statistics (Missing Papers per Month) ---")
print(df_stats)
