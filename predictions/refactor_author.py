import pandas as pd
import ast
import numpy as np

def refactor_affiliations_row(row):
    """
    Refactor affiliations string into a list of strings, one for each author.
    """
    aff_str = row['affiliations']
    authors = row['authors']
    
    if pd.isna(aff_str) or pd.isna(authors):
        return []
        
    try:
        # Convert string representation of list to actual list
        if isinstance(authors, str):
            authors_list = ast.literal_eval(authors)
        else:
            authors_list = authors
    except:
        return []

    # Mapping of author name in authors_list to their affiliations
    author_affs = {author: [] for author in authors_list}
    
    # Split the affiliation string by '; '
    parts = str(aff_str).split(';')
    
    current_author = None
    
    # Pre-calculate normalized versions for faster matching
    normalized_authors = {a.lower(): a for a in authors_list}
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
            
        if ':' in part:
            # Check if it starts with an author name in "Last, First" format
            potential_name, rest = part.split(':', 1)
            potential_name = potential_name.strip()
            
            matched_author = None
            if ',' in potential_name:
                parts_name = [x.strip() for x in potential_name.split(',', 1)]
                if len(parts_name) == 2:
                    last, first = parts_name
                    # Try to match "First Last"
                    swapped = f"{first} {last}".lower()
                    if swapped in normalized_authors:
                        matched_author = normalized_authors[swapped]
            
            if matched_author:
                current_author = matched_author
                if rest.strip():
                    author_affs[current_author].append(rest.strip())
                continue
        
        # If no colon or not a matched author name, append to current author
        if current_author:
            author_affs[current_author].append(part)
            
    # Combine affiliations for each author with '; '
    return ['; '.join(author_affs[a]) for a in authors_list]

def main():
    # Load data
    data = pd.read_csv("2025_Data.csv")
    
    # Apply refactoring
    # Each cell in 'affiliations' column will now contain a list of strings
    data['affiliations'] = data.apply(refactor_affiliations_row, axis=1)
    print(data['affiliations'][0])
    
    # Save the csv as 2025_Data_Hetansh.csv
    data.to_csv("2025_Data_Hetansh.csv", index=False)
    print("Successfully refactored affiliations and saved to 2025_Data_Hetansh.csv")

if __name__ == "__main__":
    main()