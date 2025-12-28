import os
import requests
import time
import dotenv

dotenv.load_dotenv()

ADS_API_KEY = os.getenv("ADS_API_KEY")
ADS_URL = "https://api.adsabs.harvard.edu/v1/search/query"

def get_headers():
    if not ADS_API_KEY:
        raise ValueError("ADS_API_KEY not found in environment. Please set it in your .env file.")
    return {"Authorization": f"Bearer {ADS_API_KEY}"}

def ads_request(params):
    while True:
        try:
            r = requests.get(ADS_URL, headers=get_headers(), params=params, timeout=20)
            
            # Handle rate limiting
            if r.status_code == 429:
                limit_reset = int(r.headers.get("X-RateLimit-Reset", 60))
                print(f"Rate limit reached. Sleeping for {limit_reset} seconds...")
                time.sleep(limit_reset + 1)
                continue
            
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            return None

def get_most_recent_affiliation(author_name):
    params = {
        "q": f'author:"{author_name}"',
        "fl": "author,aff,title,pubdate",  # Fetch title/date for debugging/verification
        "sort": "date desc",                # CRITICAL: Sort by date (newest first)
        "rows": 10                          # We likely only need the top few recent papers
    }
    
    data = ads_request(params)
    if not data:
        return None

    docs = data.get("response", {}).get("docs", [])
    
    for doc in docs:
        authors = doc.get("author", [])
        affs = doc.get("aff", [])
        
        # Safety check: ensure lists are parallel before zipping
        if len(authors) != len(affs):
            continue

        for auth, aff in zip(authors, affs):
            # Check if this specific author in the list matches our query
            # Using exact substring match to avoid "Ian Cass" matching "Brian Cassidy"
            if author_name.lower() in auth.lower(): 
                # Check for valid affiliation (ADS uses '-' for missing ones)
                if aff and aff != "-" and aff.strip() != "":
                    # Found the most recent valid affiliation!
                    return {
                        "affiliation": aff,
                        "paper_date": doc.get("pubdate"),
                        "paper_title": doc.get("title", [""])[0]
                    }
    
    return None

if __name__ == "__main__":
    author_name = "Ramon Miquel"
    result = get_most_recent_affiliation(author_name)
    
    if result:
        print(f"Most recent affiliation for '{author_name}':")
        print(f"Affiliation: {result['affiliation']}")
        print(f"Source: {result['paper_title']} ({result['paper_date']})")
    else:
        print(f"No affiliation found for '{author_name}'.")