import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import ast
import re
from tqdm import tqdm
import os

def clean_text(text):
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

def get_affiliations_map(soup):
    """
    Parses ltx_role_affiliation (and similar) to build a map of {key: affiliation_text}.
    Returns (map, list_of_unmapped_affiliations).
    """
    aff_map = {}
    unmapped_affs = []
    
    # Potential classes for affiliations
    # The user mentioned: ltx_contact ltx_role_affiliation / ltx_role_address / ltx_personname
    # Observations show ltx_role_affiliation is the main one.
    
    # We collect all affiliation-like blocks.
    # Note: Sometimes they are nested. valid blocks are usually 'ltx_role_affiliation' or 'ltx_role_address'
    # We essentially want to linearize the affiliation text and finding the markers.
    
    # Strategy: Find all relevant container tags.
    # In the debug example, ltx_role_affiliation contained the whole list with <sup id="...">1</sup> text <br> ...
    
    # Let's try to find high-level affiliation containers.
    # In some papers, each affiliation is a separate ltx_role_affiliation block. 
    # In others (like 2501.13056), one block contains all.
    
    affiliation_tags = soup.find_all(class_=['ltx_role_affiliation', 'ltx_role_address', 'ltx_contact'])
    
    # Filter out embedded ones if necessary? 
    # For now, let's process them.
    
    # If the text contains superscripts acting as keys, we split by them.
    
    # One common pattern: <sup class="ltx_sup">key</sup> text
    
    full_text_mode = False 
    
    for tag in affiliation_tags:
        # Check if this tag is inside another affiliation tag (dedup)
        # parents = tag.parents
        # if any(p in affiliation_tags for p in parents):
        #    continue 
            
        # Get text content with separator logic
        # Iterate over children
        current_keys = []
        current_text = []
        
        has_sups = tag.find(class_='ltx_sup') is not None
        
        if not has_sups:
            # Whole tag is one affiliation
            text = clean_text(tag.get_text())
            if text and text not in unmapped_affs:
                unmapped_affs.append(text)
            continue
            
        # If it has sups, we need to carefully extract keys.
        # This is tricky because bs4 traversal.
        
        # We will iterate through all elements in the tag
        # If we hit a sup, it starts a new affiliation (probably).
        # We save the previous one.
        
        # Determine if this tag is a "List of affiliations" or "Single affiliation with a marker".
        # If it's a single affiliation with a marker: <sup>1</sup> Univ of X
        # If it's a list: <sup>1</sup> Univ A <br> <sup>2</sup> Univ B
        
        # Let's traverse children.
        children = tag.contents
        buffer = ""
        keys = []
        
        for child in children:
            if child.name == 'sup' and 'ltx_sup' in child.get('class', []):
                # Found a key marker?
                # Check if it looks like a marker (numbers, symbols).
                marker = clean_text(child.get_text())
                # If we have existing buffer, it belongs to the previous keys (or is unmapped prefix).
                if buffer.strip():
                    if keys:
                        for k in keys:
                            aff_map[k] = clean_text(buffer)
                    else:
                        # Text before any key? strange. maybe title "Affiliations:"
                        pass
                
                # Reset
                keys = [marker]
                # Handle comma separated keys in one sup? e.g. "1,2"? usually "1" then "," then "2"
                # But in the specific file 2501.13056, the sup id was "1"
                # If the marker is "1,2", we might need to split.
                # In 2501.13056: <sup>2</sup>
                
                # Also handle <span class="ltx_text ltx_font_italic">2</span> inside sup
                
                # Clean marker: "2,4,5" or just "2"
                # The affiliation list usually has ONE key per line. 
                # Authors have MULTIPLE keys.
                # So here, we assume one key per block start.
                buffer = ""
            elif child.name == 'br':
                # Line break could mean end of current affiliation.
                pass
            else:
                # Text or other tags
                if hasattr(child, 'get_text'):
                    buffer += child.get_text()
                else:
                    buffer += str(child)
        
        # End of loop
        if buffer.strip() and keys:
            for k in keys:
                aff_map[k] = clean_text(buffer)
                
    return aff_map, unmapped_affs

def parse_html_for_affiliations(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    authors = []
    
    # 1. Build Affiliation Map
    aff_map, unmapped_affs = get_affiliations_map(soup)
    
    # 2. Extract Authors
    author_tags = soup.find_all(class_='ltx_role_author')
    
    extracted_data = []
    
    for auth_tag in author_tags:
        name_tag = auth_tag.find(class_='ltx_personname')
        if not name_tag:
            continue
            
        full_name = clean_text(name_tag.get_text())
        # The name tag text often includes the superscripts numbers "Name 1,2".
        # We should try to strip them from the name if possible, or just trust simple fuzzy matching later.
        # Actually, extracting just the text nodes excluding sup might contain the name.
        
        # Extract keys from sups
        sups = name_tag.find_all(class_='ltx_sup')
        auth_keys = []
        for sup in sups:
            # Get text, split by comma
            t = clean_text(sup.get_text())
            # Split "1,*" -> ["1", "*"]
            # Split "2,4,5" -> ["2", "4", "5"]
            parts = [x.strip() for x in re.split(r'[,;]', t) if x.strip()]
            auth_keys.extend(parts)
            
            # Remove sup text from full_name to clean it?
            # It's hard to do cleanly on the string.
            # But the 'authors' column in CSV acts as the primary key.
            # We don't need to perfect the name, just map the index if possible.
        
        # Match keys to aff_map
        my_affs = []
        for k in auth_keys:
            if k in aff_map:
                my_affs.append(aff_map[k])
                
        # If no keys, or keys not found (maybe they are unmapped?)
        if not my_affs and unmapped_affs:
            # Fallback: if only 1 unmapped affiliation, assign to all
            if len(unmapped_affs) == 1:
                my_affs = unmapped_affs[:]
            # If multiple unmapped, we can't be sure without order. 
            # Some papers list authors then affiliations in order.
            
        extracted_data.append({
            'name_raw': full_name,
            'keys': auth_keys,
            'affiliations': my_affs
        })
        
    return extracted_data

def get_row_authors(row_authors_str):
    try:
        return ast.literal_eval(row_authors_str)
    except:
        return []

def get_row_affiliations(row_affs_str):
    try:
        l = ast.literal_eval(row_affs_str)
        if hasattr(l, 'tolist'): return l.tolist()
        return l
    except:
        return []

def main():
    input_file = 'test_filled_11.csv'
    output_file = 'test_filled_12.csv'
    report_file = 'missing_affiliations_report.txt'
    
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Identify rows to process: where affiliations have 'None'
    # We need to parse valid lists first.
    
    print("Identifying target papers...")
    indices_to_process = []
    
    for idx, row in df.iterrows():
        affs_str = row['affiliations']
        affs = get_row_affiliations(affs_str)
        
        # Check if list contains None
        if affs and any(a is None for a in affs):
            indices_to_process.append(idx)
        elif not affs and row['authors']: # Empty list but has authors
            indices_to_process.append(idx)
            
    print(f"Found {len(indices_to_process)} papers with missing affiliations.")
    
    # Process
    processed_count = 0
    updated_count = 0
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    for idx in tqdm(indices_to_process):
        # Rate limit
        time.sleep(15)
        
        try:
            pdf_link = str(df.at[idx, 'pdf_link'])
            # Extract ID: arxiv.org/pdf/2501.12345 -> 2501.12345
            match = re.search(r'(\d{4}\.\d{5})', pdf_link)
            if not match:
                continue
            
            arxiv_id = match.group(0)
            target_url = f"https://arxiv.org/html/{arxiv_id}v1"
            
            response = requests.get(target_url, headers=headers)
            if response.status_code == 404:
                # Try without version? "https://arxiv.org/html/{arxiv_id}" auto-redirects usually but prompt said v1
                # If v1 fails, skip as per prompt instructions ("if it doesn't exist skip it")
                continue
            if response.status_code != 200:
                continue
                
            # Parse
            extracted = parse_html_for_affiliations(response.content)
            
            if not extracted:
                continue
                
            # Merge with existing CSV data
            # Logic: Match authors by index.
            # The extracted data list should roughly correspond to the CSV author list order.
            # The CSV author list is `['Name A', 'Name B']`.
            # `extracted` is `[{'name_raw': 'Name A 1', ...}, ...]`.
            
            csv_authors = get_row_authors(df.at[idx, 'authors'])
            csv_affs = get_row_affiliations(df.at[idx, 'affiliations'])
             
            # Initialize if None or mismatched length
            if csv_affs is None or len(csv_affs) != len(csv_authors):
                csv_affs = [None] * len(csv_authors)
            
            # Mapping attempt: Index based
            # ArXiv HTML author order usually matches metadata author order.
            
            # Truncate or pad extracted to match csv_authors length?
            # Or assume 1-to-1.
            
            limit = min(len(csv_authors), len(extracted))
            
            row_updated = False
            for i in range(limit):
                if csv_affs[i] is None:
                    # Try to fill
                    new_affs = extracted[i]['affiliations']
                    if new_affs:
                        # Join multiple affiliations with semi-colon?
                        # The CSV format seems to use strings.
                        csv_affs[i] = "; ".join(new_affs)
                        row_updated = True
                        
            if row_updated:
                df.at[idx, 'affiliations'] = str(csv_affs)
                updated_count += 1
            
            # Incremental save every 5 papers
            if (processed_count + 1) % 5 == 0:
                df.to_csv(output_file, index=False)
                
            processed_count += 1
            
        except Exception as e:
            # print(f"Error processing {idx}: {e}")
            continue

    # Final save
    print(f"Saving to {output_file}...")
    df.to_csv(output_file, index=False)
    
    # Generate statistics for report
    total_papers = len(df)
    fully_filled = 0
    partially_filled = 0
    total_nulls = 0
    incomplete_indices = []
    
    for idx, row in df.iterrows():
        affs = get_row_affiliations(row['affiliations'])
        auths = get_row_authors(row['authors'])
        
        if not affs:
            # If no affiliations list but authors exist, it's incomplete
            if auths:
                partially_filled += 1 # Or "Empty"?
                total_nulls += len(auths)
                incomplete_indices.append(idx + 1) # 1-based index for report
            else:
                fully_filled += 1 # No authors -> fully filled (trivial)
            continue
            
        null_count = sum(1 for a in affs if a is None)
        total_nulls += null_count
        
        if null_count == 0:
            fully_filled += 1
        else:
            partially_filled += 1
            incomplete_indices.append(idx + 1)

    print("Generating report...")
    with open(report_file, 'w') as f:
        f.write("=== MISSING AFFILIATIONS REPORT (V12) ===\n\n")
        f.write(f"Total papers: {total_papers}\n")
        f.write(f"Fully filled papers: {fully_filled}\n")
        f.write(f"Partially filled papers: {partially_filled}\n")
        f.write(f"Total remaining null author affiliations: {total_nulls}\n\n")
        f.write(f"Remaining incomplete indices ({len(incomplete_indices)}):\n")
        f.write(str(incomplete_indices))
        f.write("\n")
        
    print("Done.")

if __name__ == "__main__":
    main()
