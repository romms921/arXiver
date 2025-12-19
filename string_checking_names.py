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
    
    # helper to extract ID from arxiv.org/pdf/2501.00089 strip versioning
    def extract_id_from_url(url):
        if pd.isna(url): return ""
        # Get filename part
        part = str(url).split('/')[-1]
        # Match standard arxiv ID pattern (digits.digits)
        # e.g. 2501.00089v1 -> 2501.00089
        match = re.search(r'(\d{4}\.\d{4,5})', part)
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
# These counts represent the number of "New" (pure astro-ph) papers per month for 2025
check = [1344, 1287, 1585, 1467, 1386, 1478, 1816, 1304, 1860, 1889, 1551, 1182]
months = range(1, 13)

# Use dash in the URL as specified (e.g., 2025-01) and show=2000 to get all papers at once
base_url = "https://arxiv.org/list/astro-ph/2025-{:02d}?show=2000"

print("\nStarting to scan arXiv for missing astro-ph papers...")

for month in months:
    month_str = f"2025-{month:02d}"
    papers_to_process = check[month-1]
    url = base_url.format(month)
    
    print(f"[{month_str}] Fetching list (Targeting {papers_to_process} pure papers)...")
    month_missing_count = 0
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(f"  Failed! Status code: {response.status_code}")
            stats.append({'month': month_str, 'Missing_Count': 0, 'Error': f"HTTP {response.status_code}"})
            continue
        
        soup = bs4.BeautifulSoup(response.content, 'html.parser')
        
        # arXiv list pages use <dt> for ID and <dd> for metadata/title
        dts = soup.find_all('dt')
        dds = soup.find_all('dd')
        
        if not dts:
            print(f"  No papers found for {month_str} (structure may have changed).")
            stats.append({'month': month_str, 'Missing_Count': 0})
            continue
            
        print(f"  Found {len(dts)} total entries. Processing top {papers_to_process} pure astro-ph papers...")
        
        # Iterate over the first N papers as they are the "New" (pure) submissions
        # Any entries after this index are typically cross-lists or replacements
        for i, (dt, dd) in enumerate(zip(dts[:papers_to_process], dds[:papers_to_process])):
            # Extract ID from <dt>
            anchor = dt.find('a', title='Abstract')
            if not anchor:
                continue
            
            id_text = anchor.get_text().strip()
            clean_id = id_text.replace('arXiv:', '').strip()
            
            # Compare with existing IDs
            if clean_id not in existing_ids:
                # Extract Title from <dd>
                title_div = dd.find('div', class_='list-title mathjax')
                if title_div:
                    raw_title = title_div.get_text().strip()
                    title_text = raw_title.replace("Title:", "").strip()
                else:
                    title_text = "Unknown Title"
                    
                missing_papers.append({'title': title_text, 'month': month_str, 'id': clean_id})
                month_missing_count += 1
            
        stats.append({'month': month_str, 'Missing_Count': month_missing_count})
        print(f"  Finished {month_str}: Found {month_missing_count} missing papers.")

    except Exception as e:
        print(f"  Error processing {month_str}: {e}")
        stats.append({'month': month_str, 'Missing_Count': 0, 'Error': str(e)})

    # Crawl delay of 15 seconds between different months (pages)
    if month < 12:
        print("  Waiting 15 seconds for crawl delay...")
        time.sleep(15)

# Save results
missing_csv = "months_missing_papers.csv"
stats_csv = "missing_papers_stats.csv"

if missing_papers:
    df_missing = pd.DataFrame(missing_papers)
    df_missing.to_csv(missing_csv, index=False)
    print(f"\nSaved {len(df_missing)} total missing papers to {missing_csv}")
else:
    print(f"\nNo missing papers found.")
    pd.DataFrame(columns=['title', 'month', 'id']).to_csv(missing_csv, index=False)

# Save Stats Summary
df_stats = pd.DataFrame(stats)
df_stats.to_csv(stats_csv, index=False)
print(f"Saved monthly stats to {stats_csv}")

print("\n--- Statistics (Missing Papers per Month) ---")
print(df_stats)

