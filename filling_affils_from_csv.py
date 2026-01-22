import pandas as pd
import requests
import os
import time
import dotenv
dotenv.load_dotenv()

# ==========================================
# CONFIGURATION
# ==========================================
# Make sure your API key is set in your terminal: export ADS_API_KEY="your_key"
# Or paste it here temporarily:
ADS_API_KEY = os.getenv("ADS_API_KEY") 
ADS_URL = "https://api.adsabs.harvard.edu/v1/search/query"

INPUT_FILE = 'missed_affils_1.csv'
OUTPUT_FILE = 'missed_affils_completed.csv'

# ==========================================
# CORE FUNCTIONS
# ==========================================

def get_affiliation(author_name, api_key):
    """
    Queries NASA ADS for the author's most recent paper to find affiliation.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Logic: Search for author, sort by date desc, get top 1 result
    params = {
        "q": f'author:"{author_name}"',
        "fl": "author,aff,bibcode,year",
        "sort": "date desc",
        "rows": 1
    }

    try:
        response = requests.get(ADS_URL, headers=headers, params=params, timeout=10)
        
        if response.status_code == 401:
            return "ERROR_AUTH" # Signal to stop script if key is bad
        if response.status_code == 429:
            time.sleep(60) # Rate limit hit, wait 1 min
            return get_affiliation(author_name, api_key) # Retry
        if response.status_code != 200:
            return f"API_Error_{response.status_code}"

        data = response.json()
        docs = data.get('response', {}).get('docs', [])

        if not docs:
            return "Author Not Found in ADS"

        paper = docs[0]
        ads_authors = paper.get('author', [])
        ads_affils = paper.get('aff', [])

        if not ads_affils:
            return "No Affiliation in Record"

        # === MATCHING LOGIC ===
        # ADS returns lists: author=['Doe, J', 'Smith, A'], aff=['Univ A', 'Univ B']
        # We need to map the input name to the correct index.
        
        # 1. Extract last name from input (e.g., "John K. Nino" -> "nino")
        input_parts = author_name.replace('.', ' ').split()
        if not input_parts: return "Invalid Name"
        target_last = input_parts[-1].lower().strip()

        match_index = -1
        
        # 2. Find that last name in the ADS author list
        for i, auth_str in enumerate(ads_authors):
            # ADS Name format: "Nino, John K."
            ads_last = auth_str.split(',')[0].lower().strip()
            
            if ads_last == target_last:
                match_index = i
                break
        
        # 3. Retrieve affiliation
        if match_index != -1:
            if match_index < len(ads_affils):
                aff = ads_affils[match_index]
                return aff if aff != '-' else "Affiliation Missing (-)"
            else:
                return "Affiliation List Mismatch"
        
        # Fallback: If single author paper, take the first affiliation
        if len(ads_authors) == 1 and len(ads_affils) > 0:
            return ads_affils[0]

        return "Name Mismatch in Paper"

    except Exception as e:
        return f"Error: {str(e)}"

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    if not ADS_API_KEY:
        print("❌ CRITICAL: ADS_API_KEY is missing.")
        return

    print(f"Reading {INPUT_FILE}...")
    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Clean the affiliation column (treat string 'Not Found' as updateable)
    # Some rows in your file have actual affiliations; we must preserve those.
    total_rows = len(df)
    print(f"Loaded {total_rows} authors.")

    updates_made = 0
    
    try:
        for index, row in df.iterrows():
            author = row['Author']
            current_affil = str(row['Affiliation']).strip()

            # Skip if we already have an affiliation (e.g., Yogesh, Bin Chen)
            if current_affil not in ['Not Found', 'nan', 'None', '']:
                continue

            print(f"[{index+1}/{total_rows}] Searching: {author}...", end="\r")
            
            new_affil = get_affiliation(author, ADS_API_KEY)
            
            if new_affil == "ERROR_AUTH":
                print("\n❌ API Key rejected (401). Stopping.")
                break

            # Clean newline characters from ADS results to prevent CSV corruption
            if new_affil:
                new_affil = new_affil.replace('\n', ' ').replace('\r', '')

            df.at[index, 'Affiliation'] = new_affil
            updates_made += 1

            # Politeness sleep (ADS limit is usually high, but let's be safe)
            time.sleep(0.3)

            # SAVE PROGRESS every 20 rows
            if updates_made % 20 == 0:
                df.to_csv(OUTPUT_FILE, index=False)

    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Saving current progress...")

    # Final Save
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n\n✅ Done! Processed {updates_made} authors.")
    print(f"Results saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()