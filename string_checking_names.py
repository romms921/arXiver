import pandas as pd
import requests
import bs4
import time
import re
import os

# Load existing data
csv_name = "arxiv_papers_copy.csv"
print(f"Loading existing papers from {csv_name}...")
try:
    data = pd.read_csv(csv_name)
    
    # helper to extract ID from arxiv.org/pdf/2501.00089 strip versioning
    def extract_id_from_url(url):
        if pd.isna(url): return ""
        part = str(url).split('/')[-1]
        match = re.search(r'(\d{4}\.\d{4,5})', part)
        if match:
            return match.group(1)
        return part # fallback

    # Create mapping of ID -> DataFrame Index for updates
    id_to_idx = {}
    for idx, row in data.iterrows():
        clean_id = extract_id_from_url(row['pdf_link'])
        if clean_id:
            id_to_idx[clean_id] = idx
            
    print(f"Loaded {len(id_to_idx)} unique IDs from CSV.")

except FileNotFoundError:
    print(f"Error: {csv_name} not found.")
    exit()

missing_papers = []
stats = []
migration_details = [] # List of {'id': id, 'from': old_month, 'to': new_month}
verified_indices = set() # Track which rows in CSV were matched to an ArXiv list

# These counts represent the number of "New" (pure astro-ph) papers per month for 2025
check = [1344, 1287, 1585, 1467, 1386, 1478, 1816, 1304, 1860, 1889, 1551, 1305]
months = range(1, 13)

base_url = "https://arxiv.org/list/astro-ph/2025-{:02d}?show=2000"

print("\nStarting to scan arXiv for missing/misdated astro-ph papers...")

for month in months:
    month_str = f"2025-{month:02d}"
    papers_to_process = check[month-1]
    url = base_url.format(month)
    
    print(f"[{month_str}] Fetching list (Targeting {papers_to_process} pure papers)...")
    month_missing_count = 0
    month_shift_count = 0
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(f"  Failed! Status code: {response.status_code}")
            stats.append({'month': month_str, 'Missing_Count': 0, 'Shift_Count': 0})
            continue
        
        soup = bs4.BeautifulSoup(response.content, 'html.parser')
        dts = soup.find_all('dt')
        dds = soup.find_all('dd')
        
        # Iterate over the "New" papers from ArXiv
        for i, (dt, dd) in enumerate(zip(dts[:papers_to_process], dds[:papers_to_process])):
            anchor = dt.find('a', title='Abstract')
            if not anchor: continue
            
            clean_id = anchor.get_text().replace('arXiv:', '').strip()
            
            if clean_id in id_to_idx:
                idx = id_to_idx[clean_id]
                verified_indices.add(idx)
                current_date = str(data.at[idx, 'date'])
                
                # If current month doesn't match the listing month
                if not current_date.startswith(month_str):
                    old_month = current_date[:7] # e.g. "2025-05"
                    data.at[idx, 'date'] = f"{month_str}-01"
                    migration_details.append({'id': clean_id, 'from': old_month, 'to': month_str})
                    month_shift_count += 1
            else:
                title_div = dd.find('div', class_='list-title mathjax')
                title_text = title_div.get_text().replace("Title:", "").strip() if title_div else "Unknown Title"
                missing_papers.append({'title': title_text, 'month': month_str, 'id': clean_id})
                month_missing_count += 1
            
        stats.append({'month': month_str, 'Missing_Count': month_missing_count, 'Shift_Count': month_shift_count})
        print(f"  Finished {month_str}: {month_missing_count} missing, {month_shift_count} shifted.")

    except Exception as e:
        print(f"  Error processing {month_str}: {e}")

    if month < 12:
        time.sleep(15)

# Save corrected data
data.to_csv(csv_name, index=False)

# Analysis of residual papers (unverified)
unverified_counts = {}
for idx, row in data.iterrows():
    if idx not in verified_indices:
        m = str(row['date'])[:7]
        unverified_counts[m] = unverified_counts.get(m, 0) + 1

# Print Migration Report
print("\n" + "="*50)
print("DETAILED MIGRATION REPORT")
print("="*50)

if migration_details:
    df_mig = pd.DataFrame(migration_details)
    summary = df_mig.groupby(['from', 'to']).size().reset_index(name='count')
    print("Papers shifted between months:")
    print(summary.to_string(index=False))
else:
    print("No papers were shifted.")

print("\n" + "="*50)
print("DISCREPANCY ANALYSIS (Papers in CSV not in ArXiv Pure Lists)")
print("="*50)
print("These papers remain in the CSV under these months but weren't in the 'New' lists:")
for m in sorted(unverified_counts.keys()):
    if m.startswith("2025"):
        print(f"  {m}: {unverified_counts[m]} extra papers")

print("\nSaved updated CSV and missing papers log.")

