import pandas as pd
import json
import os
import re
import ast

def get_keyword_bank():
    json_path = os.path.join(os.path.dirname(__file__), 'uat.json')
    if not os.path.exists(json_path):
        print(f"Error: {json_path} not found.")
        return []

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    keywords = set()
    # Keys for labels in UAT (SKOS/RDF)
    label_keys = [
        "http://www.w3.org/2004/02/skos/core#prefLabel",
        "http://www.w3.org/2004/02/skos/core#altLabel",
        "http://www.w3.org/2000/01/rdf-schema#label"
    ]

    for concept_uri, properties in data.items():
        for key in label_keys:
            if key in properties:
                for item in properties[key]:
                    # User requirement: filter "value" field where "type" : "literal"
                    if isinstance(item, dict) and item.get("type") == "literal":
                        val = item.get("value")
                        if val:
                            keywords.add(val)
    
    return list(keywords)

def recommend_keywords(title, abstract, bank):
    # Combine title and abstract
    text = f"{str(title)} {str(abstract)}".lower()
    
    # Extract unique tokens from text (bag of words)
    # Using regex to match alphanumeric words, effectively ignoring punctuation
    text_tokens = set(re.findall(r'\b\w+\b', text))
    
    matches = []
    # Check each keyword
    for kw in bank:
        # Tokenize the keyword phrase
        kw_tokens = re.findall(r'\b\w+\b', kw.lower())
        
        if not kw_tokens:
            continue
            
        # Optimization: Fast check with sets
        # If all tokens in the keyword phrase appear in the text tokens (regardless of order)
        if set(kw_tokens).issubset(text_tokens):
            matches.append(kw)
            
    # Return all unique matches
    return sorted(list(set(matches)))[:3]

def main():
    csv_path = '2025_Data_missing.csv'
    if not os.path.exists(csv_path):
        print(f"File {csv_path} not found.")
        return

    print("Loading data...")
    df = pd.read_csv(csv_path)
    
    print("Loading keyword bank from uat.json...")
    bank = get_keyword_bank()
    print(f"Loaded {len(bank)} unique keywords from UAT.")

    print("Generating smart keywords for all papers...")
    # Apply recommendation logic
    # depending on dataset size, this might take a moment
    smart_keywords_column = []
    
    total_rows = len(df)
    for index, row in df.iterrows():
        if index % 100 == 0:
            print(f"Processing row {index}/{total_rows}...")
            
        recs = recommend_keywords(row['title'], row['abstract'], bank)
        smart_keywords_column.append(str(recs)) # Store as string representation of list to match csv format

    df['smart_keywords'] = smart_keywords_column
    
    print("Saving updated CSV...")
    df.to_csv(csv_path, index=False)
    print("Done! 'smart_keywords' column added.")

if __name__ == "__main__":
    main()
