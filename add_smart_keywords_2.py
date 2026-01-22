import pandas as pd
import os
import re

def get_keyword_bank():
    dat_path = os.path.join(os.path.dirname(__file__), 'top_500_keywords.dat')
    if not os.path.exists(dat_path):
        print(f"Error: {dat_path} not found.")
        return []

    keywords = set()
    with open(dat_path, 'r', encoding='utf-8') as f:
        for line in f:
            # Assuming one keyword per line. Strip whitespace.
            kw = line.strip()
            if kw:
                keywords.add(kw)
    
    return list(keywords)

def recommend_keywords(title, abstract, bank):
    # Combine title and abstract
    text = f"{str(title)} {str(abstract)}".lower()
    
    matches = []
    # Check each keyword
    for kw in bank:
        kw_lower = kw.lower()
        
        # Search for the exact phrase in the text
        # We use regex word boundaries (\b) to ensure we don't match partial words
        # e.g., ensuring "star" doesn't match inside "starting"
        if re.search(r'\b' + re.escape(kw_lower) + r'\b', text):
            matches.append(kw)
            
    # Return all unique matches
    return sorted(list(set(matches)))[:3]

def main():
    csv_path = '.csv'
    if not os.path.exists(csv_path):
        print(f"File {csv_path} not found.")
        return

    print("Loading data...")
    df = pd.read_csv(csv_path)
    
    print("Loading keyword bank from top_500_keywords.dat...")
    bank = get_keyword_bank()
    print(f"Loaded {len(bank)} unique keywords.")

    print("Generating smart keywords for all papers...")
    # Apply recommendation logic
    # depending on dataset size, this might take a moment
    smart_keywords_column = []
    
    total_rows = len(df)
    for index, row in df.iterrows():
        if index % 100 == 0:
            print(f"Processing row {index}/{total_rows}...")

        # Already has keywords
        if pd.notna(row.get('keywords', None)) and str(row['keywords']).strip():
            smart_keywords_column.append(str(row['keywords']))
            continue
        recs = recommend_keywords(row['title'], row['abstract'], bank)
        smart_keywords_column.append(str(recs)) # Store as string representation of list to match csv format

    df['smart_keywords_2'] = smart_keywords_column
    
    print("Saving updated CSV...")
    df.to_csv(csv_path, index=False)
    print("Done! 'smart_keywords_2' column added.")
if __name__ == "__main__":
    main()
