import pandas as pd
import requests
import time
import os
from tqdm import tqdm


data = pd.read_csv('FINAL_ARXIV_2025_copy.csv')

# --- CONFIGURATION ---
S2_API_KEY = None  # Add your key here if you have one
OUTPUT_FILENAME = "s2_checkpoint.csv"
BATCH_SIZE = 30 

HEADERS = {"x-api-key": S2_API_KEY} if S2_API_KEY else {}
URL_BATCH = "https://api.semanticscholar.org/graph/v1/paper/batch"

# --- HELPER FUNCTIONS ---

def get_arxiv_id(link):
    """Extracts ArXiv ID from a link string."""
    if pd.isna(link): return None
    try:
        link = str(link)
        if "arxiv.org" in link:
            # Extract ID part: https://arxiv.org/pdf/1706.03762.pdf -> 1706.03762
            id_part = link.split('/')[-1].replace('.pdf', '')
            return f"ARXIV:{id_part}"
    except:
        pass
    return None

def calculate_metrics(paper_data):
    """Calculates citations and non-self-citations."""
    if not paper_data:
        return 0, 0

    # 1. Total Citations
    total_cites = paper_data.get('citationCount', 0)

    # 2. Non-Self Citations
    non_self_cites = 0
    
    # Get original author IDs
    original_author_ids = set()
    if paper_data.get('authors'):
        original_author_ids = {a['authorId'] for a in paper_data['authors'] if a['authorId']}

    citations = paper_data.get('citations', [])
    if citations is None: citations = [] 

    for citing_paper in citations:
        is_self_cite = False
        if citing_paper.get('authors'):
            citing_author_ids = {a['authorId'] for a in citing_paper['authors'] if a['authorId']}
            # Check overlap
            if not original_author_ids.isdisjoint(citing_author_ids):
                is_self_cite = True
        
        if not is_self_cite:
            non_self_cites += 1

    return total_cites, non_self_cites

# --- MAIN EXECUTION ---

# 1. Load Data (assuming 'data' dataframe exists)
# data = pd.read_csv('your_file.csv') # Uncomment if needed

print("Generating IDs...")
data['s2_id'] = data['pdf_link'].apply(get_arxiv_id)

# Filter for valid IDs
valid_data = data[data['s2_id'].notna()].copy()
print(f"Found {len(valid_data)} papers with valid ArXiv IDs.")

# 2. Resume Logic
processed_ids = set()
if os.path.exists(OUTPUT_FILENAME):
    print(f"Found existing checkpoint. Resuming...")
    existing_df = pd.read_csv(OUTPUT_FILENAME)
    if 's2_id' in existing_df.columns:
        processed_ids = set(existing_df['s2_id'].unique())
    print(f"Skipping {len(processed_ids)} already processed papers.")

# Filter list to process
ids_to_process = [x for x in valid_data['s2_id'].unique() if x not in processed_ids]

# 3. Batch Processing
# Removed 'authors.affiliations' from fields to speed up and simplify
fields = "title,citationCount,citations.authors"

print(f"Starting batch processing for {len(ids_to_process)} papers...")

for i in tqdm(range(0, len(ids_to_process), BATCH_SIZE), desc="Processing Batches"):
    batch_ids = ids_to_process[i : i + BATCH_SIZE]
    batch_results = []
    
    try:
        r = requests.post(f"{URL_BATCH}?fields={fields}", json={"ids": batch_ids}, headers=HEADERS)
        
        if r.status_code == 200:
            responses = r.json()
            
            for request_id, paper_data in zip(batch_ids, responses):
                # Retrieve title from API response for verification
                # If paper_data is None (ID not found), title will be "Not Found"
                api_title = paper_data.get('title', 'N/A') if paper_data else 'Paper Not Found'
                
                cites, non_self = calculate_metrics(paper_data)
                
                batch_results.append({
                    's2_id': request_id,
                    'S2_Title': api_title,  # Added Title here
                    'S2_Citations': cites,
                    'S2_NonSelfCitations': non_self
                })
                
        elif r.status_code == 429:
            print(f"\nRate limit hit. Sleeping for 60s...")
            time.sleep(30)
            continue
        else:
            print(f"\nError {r.status_code}: {r.text}")
    
        time.sleep(5)  # Brief pause between batches

    except Exception as e:
        print(f"\nException: {e}")

    # 4. Save to CSV
    if batch_results:
        batch_df = pd.DataFrame(batch_results)
        header_mode = not os.path.exists(OUTPUT_FILENAME)
        batch_df.to_csv(OUTPUT_FILENAME, mode='a', header=header_mode, index=False)

    if not S2_API_KEY:
        time.sleep(5)

print("\nProcessing complete. Check 's2_checkpoint.csv' for results.")