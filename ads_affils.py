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

PATH = 'none_affil_authors.txt'
OUTPUT_PATH = "missed_affils_1.csv"

# Batching and Throttling
BATCH_SIZE = 10
RETRY_WAIT = 60


# --- HELPER FUNCTIONS ---

def get_author_affiliation_ads(author_name):
    """
    Queries ADS for the most recent paper by the given author
    and attempts to retrieve their affiliation.
    """
    if not ADS_API_KEY:
        raise ValueError("ADS_API_KEY is not set.")

    # Search for papers by this author, sorted by date (descending), limit to 1 result
    # We ask for a specific field list: author, aff, pubdate, bibcode
    params = {
        "q": f'author:"{author_name}"',
        "fl": "author,aff,pubdate,bibcode",
        "sort": "date desc",
        "rows": 1
    }
    headers = {
        "Authorization": f"Bearer {ADS_API_KEY}"
    }

    try:
        response = requests.get(ADS_URL, headers=headers, params=params)
        
        # Handle Rate Limits 
        if response.status_code == 429:
            print(f"Rate limit hit. Waiting {RETRY_WAIT} seconds...")
            time.sleep(RETRY_WAIT)
            return get_author_affiliation_ads(author_name) # Retry once
        
        response.raise_for_status()
        data = response.json()
        
        docs = data.get("response", {}).get("docs", [])
        if not docs:
            return None

        doc = docs[0]
        authors = doc.get("author", [])
        affiliations = doc.get("aff", [])

        # The 'aff' list corresponds index-wise to the 'author' list.
        # We need to find the index of the queried author in the returned author list.
        # Note: ADS author matching can be fuzzy. We attempt a simple match.
        
        # Normalize name for basic matching (remove extra spaces, lower case)
        target_name_normalized = re.sub(r'\s+', ' ', author_name).strip().lower()

        found_affiliation = None
        
        for idx, auth in enumerate(authors):
            # Clean up the returned author name
            auth_normalized = re.sub(r'\s+', ' ', auth).strip().lower()
            
            # Simple substring check (e.g. "Smith, J." in "Smith, John")
            if target_name_normalized in auth_normalized or auth_normalized in target_name_normalized:
                if idx < len(affiliations):
                    found_affiliation = affiliations[idx]
                    # ADS uses '-' for null affiliations sometimes
                    if found_affiliation == '-':
                        found_affiliation = None
                    break
        
        return found_affiliation

    except Exception as e:
        print(f"Error fetching data for {author_name}: {e}")
        return None

# --- MAIN EXECUTION ---

def main():
    if not os.path.exists(PATH):
        print(f"Input file {PATH} not found.")
        return

    # Read author names
    with open(PATH, 'r', encoding='utf-8') as f:
        authors = [line.strip() for line in f if line.strip()]

    results = []

    print(f"Processing {len(authors)} authors...")

    for author in tqdm(authors):
        affiliation = get_author_affiliation_ads(author)
        results.append({
            "Author": author,
            "Affiliation": affiliation if affiliation else "Not Found"
        })
        # Be polite to the API
        time.sleep(0.5)

    # Save results
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved results to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()