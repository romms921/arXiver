import re
import csv

def extract_to_csv(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split the file into individual paper blocks
    papers = re.split(r'-{80}\nPAPER: ', content)
    
    all_data = []

    for block in papers:
        if not block.strip() or "ERROR: Failed to download" in block:
            continue
            
        # Extract Title (first line of the block)
        lines = block.split('\n')
        title = lines[0].strip()
        
        # Locate the Affiliation Section
        if "AFFILIATION SECTION:" not in block:
            continue
            
        latex_content = block.split("AFFILIATION SECTION:")[1].strip()

        # --- PARSING LOGIC ---
        
        # 1. Look for AAS Style (\author{Name} \affiliation{Affil})
        authors = re.findall(r'\\author(?:\[.*?\])?\{(.*?)\}', latex_content)
        affils = re.findall(r'\\affiliation\{(.*?)\}', latex_content)
        
        if authors and affils:
            # Simple 1-to-1 mapping or group mapping
            for i in range(len(authors)):
                # Clean LaTeX commands from names
                name = re.sub(r'\\[a-zA-Z]+|\$|\{|\}', '', authors[i]).strip()
                # Use corresponding affiliation or last one available
                aff_text = affils[i] if i < len(affils) else affils[-1]
                clean_aff = re.sub(r'\s+', ' ', aff_text).strip()
                
                all_data.append([title, name, clean_aff, "Complete"])
                
        # 2. Look for A&A Style (\author{Name \inst{1}} \institute{1... \and 2...})
        else:
            inst_section = re.search(r'\\institute\{(.*?)\}(?:\s*\\|$)', latex_content, re.DOTALL)
            if inst_section:
                inst_text = inst_section.group(1)
                # Split institutes by \and
                institutes = [re.sub(r'\s+', ' ', i).strip() for i in re.split(r'\\and', inst_text)]
                
                # Find authors and their \inst tags
                auth_matches = re.findall(r'\\author\{(.*?)\}', latex_content, re.DOTALL)
                if auth_matches:
                    for auth_block in auth_matches:
                        # Split multiple authors in one tag
                        for entry in re.split(r'\\and|,', auth_block):
                            name = re.sub(r'\\inst\{.*?\}|\$|\\.*?(\s+|$)', '', entry).strip()
                            if not name: continue
                            
                            # Find the index numbers
                            idx_match = re.search(r'\\inst\{(.*?)\}', entry)
                            if idx_match:
                                indices = [int(i.strip()) for i in idx_match.group(1).split(',') if i.strip().isdigit()]
                                # Join matching institutes
                                matched_affs = [institutes[i-1] for i in indices if 0 < i <= len(institutes)]
                                aff_string = "; ".join(matched_affs)
                            else:
                                aff_string = "N/A"
                                
                            all_data.append([title, name, aff_string, "Complete" if aff_string != "N/A" else "Incomplete"])

    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Title", "Author", "Affiliations", "Status"])
        writer.writerows(all_data)

    print(f"Extraction complete! Saved {len(all_data)} rows to {output_file}")

# To run: ensure your file is named exactly this or change the string below
extract_to_csv('latex_affiliations_output.txt', 'full_affiliations_results.csv')