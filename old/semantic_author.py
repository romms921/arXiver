import pandas as pd
import requests
import time
import os
from tqdm import tqdm

data = pd.read_csv('FINAL_ARXIV_2025_copy.csv')

# --- CONFIGURATION ---
S2_API_KEY = None  # Add your key here e.g., "AbCdEf123456"
OUTPUT_FILENAME = "s2_checkpoint.csv"
BATCH_SIZE = 30 # Safe size for heavy payloads

# API Setup
HEADERS = {"x-api-key": S2_API_KEY} if S2_API_KEY else {}
URL_BATCH = "https://api.semanticscholar.org/graph/v1/paper/batch"

# --- HELPER FUNCTIONS ---

def get_arxiv_id(link):
    """Extracts ArXiv ID from a link string."""
    if pd.isna(link): return None
    try:
        # Standardize link to string just in case
        link = str(link)
        if "arxiv.org" in link:
            # Extract ID part: https://arxiv.org/pdf/1706.03762.pdf -> 1706.03762
            id_part = link.split('/')[-1].replace('.pdf', '')
            # Clean version suffix (e.g. v1) if present, though S2 usually handles it
            return f"ARXIV:{id_part}"
    except:
        pass
    return None

def calculate_metrics(paper_data):
    """Calculates the 3 requested metrics from the raw API response."""
    if not paper_data:
        return "Not Found", 0, 0

    # 1. Author Affiliations
    affiliations = set()
    if 'authors' in paper_data and paper_data['authors']:
        for author in paper_data['authors']:
            if author.get('affiliations'):
                for aff in author['affiliations']:
                    # Some affiliations are objects, some strings. Handle both.
                    affiliations.add(str(aff))
    aff_str = "; ".join(affiliations) if affiliations else "None"

    # 2. Total Citations
    total_cites = paper_data.get('citationCount', 0)

    # 3. Non-Self Citations
    non_self_cites = 0
    
    # Get original author IDs (the authors of the paper we are analyzing)
    original_author_ids = set()
    if paper_data.get('authors'):
        original_author_ids = {a['authorId'] for a in paper_data['authors'] if a['authorId']}

    citations = paper_data.get('citations', [])
    if citations is None: citations = [] # Handle NoneType return

    for citing_paper in citations:
        is_self_cite = False
        if citing_paper.get('authors'):
            citing_author_ids = {a['authorId'] for a in citing_paper['authors'] if a['authorId']}
            # If there is ANY overlap between original authors and citing authors
            if not original_author_ids.isdisjoint(citing_author_ids):
                is_self_cite = True
        
        if not is_self_cite:
            non_self_cites += 1

    return aff_str, total_cites, non_self_cites

# --- MAIN EXECUTION ---

# 1. Load your data (Assuming you have this loaded already)
# data = pd.read_csv('your_dataset.csv') 

# 2. Generate S2 IDs
print("Generating IDs...")
data['s2_id'] = data['pdf_link'].apply(get_arxiv_id)

# Filter for valid IDs
valid_data = data[data['s2_id'].notna()].copy()
print(f"Found {len(valid_data)} papers with valid ArXiv IDs.")

# 3. Check for existing progress (Resume Logic)
processed_ids = set()
if os.path.exists(OUTPUT_FILENAME):
    print(f"Found existing checkpoint file: {OUTPUT_FILENAME}. Resuming...")
    existing_df = pd.read_csv(OUTPUT_FILENAME)
    # Assume the checkpoint has an 's2_id' column
    if 's2_id' in existing_df.columns:
        processed_ids = set(existing_df['s2_id'].unique())
    print(f"Skipping {len(processed_ids)} already processed papers.")

# Filter out IDs that are already done
ids_to_process = [x for x in valid_data['s2_id'].unique() if x not in processed_ids]

# 4. Batch Processing Loop
fields = "title,authors.affiliations,citationCount,citations.authors"

print(f"Starting batch processing for {len(ids_to_process)} papers...")

# Iterate in chunks
for i in tqdm(range(0, len(ids_to_process), BATCH_SIZE), desc="Processing Batches"):
    batch_ids = ids_to_process[i : i + BATCH_SIZE]
    
    # Storage for this specific batch
    batch_results = []
    
    try:
        # API Request
        r = requests.post(f"{URL_BATCH}?fields={fields}", json={"ids": batch_ids}, headers=HEADERS)
        
        if r.status_code == 200:
            responses = r.json()
            
            # Match responses to IDs
            # S2 returns list in same order as request
            for request_id, paper_data in zip(batch_ids, responses):
                
                aff, cites, non_self = calculate_metrics(paper_data)
                
                batch_results.append({
                    's2_id': request_id,
                    'S2_Affiliations': aff,
                    'S2_Citations': cites,
                    'S2_NonSelfCitations': non_self
                })
                
        elif r.status_code == 429:
            print(f"\nRate limit hit. Sleeping for 60s...")
            time.sleep(60)
            # In a real app, you might want to retry this batch. 
            # Here we skip to keep logic simple, but since we didn't save, 
            # re-running the script will catch it next time.
            continue
            
        else:
            print(f"\nError {r.status_code}: {r.text}")
            
    except Exception as e:
        print(f"\nException: {e}")

    # 5. Save Progress Immediately
    if batch_results:
        batch_df = pd.DataFrame(batch_results)
        
        # If file doesn't exist, write with header. If it does, append without header.
        header_mode = not os.path.exists(OUTPUT_FILENAME)
        batch_df.to_csv(OUTPUT_FILENAME, mode='a', header=header_mode, index=False)

    # Polite delay between batches if no API key
    if not S2_API_KEY:
        time.sleep(1)

# --- FINAL MERGE ---
print("\nProcessing complete. Merging results back to original data...")

# Reload the full checkpoint file
if os.path.exists(OUTPUT_FILENAME):
    results_df = pd.read_csv(OUTPUT_FILENAME)
    
    # Merge on s2_id
    # We use 'left' merge to keep all original rows, even those that failed or had no ID
    final_df = pd.merge(data, results_df, on='s2_id', how='left')
    
    print("Merge successful.")
    print(final_df[['title', 'S2_Citations', 'S2_NonSelfCitations']].head())
    
    # Optional: Save the final complete dataset
    # final_df.to_csv("final_complete_dataset.csv", index=False)
else:
    print("No results file found.")