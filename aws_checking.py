import xml.etree.ElementTree as ET
import pandas as pd

# 1. Load your data
# Adjust path as necessary
data = pd.read_csv('/Users/ainsleylewis/Documents/Astronomy/arXiver/2025_Data.csv')

# --- CLEANING STEP ---
# Ensure we get a clean ID. 
# 1. Split by '/' to handle full URLs.
# 2. Remove '.pdf' if it exists at the end of the string.
# 3. Remove version numbers (e.g., v1, v2) if they exist (optional, but safer).
def clean_id(link_str):
    if not isinstance(link_str, str): return ""
    # Get the last part of URL: "http://.../2101.00123.pdf" -> "2101.00123.pdf"
    aid = link_str.split('/')[-1]
    # Remove .pdf
    aid = aid.replace('.pdf', '')
    # Remove version suffix like v1, v2 (Manifest usually just lists the base ID)
    aid = aid.split('v')[0] 
    return aid

data['arxiv_id'] = data['pdf_link'].apply(clean_id)

# Filter out empty strings if any bad rows existed
my_ids = set(x for x in data['arxiv_id'].tolist() if x)
print(f"Loaded {len(my_ids)} unique IDs to search.")

# 2. Parse the manifest
# Make sure this XML file is in the same folder, or provide full path
try:
    tree = ET.parse('arXiv_src_manifest.xml')
    root = tree.getroot()
except FileNotFoundError:
    print("Error: arXiv_src_manifest.xml not found. Please download it via S3 first.")
    exit()

# 3. Build a lookup index (Chunk -> Start/End ID)
chunks = []
for file_node in root.findall('file'):
    filename = file_node.find('filename').text
    first_id = file_node.find('first_item').text
    last_id = file_node.find('last_item').text
    chunks.append({'tar': filename, 'start': first_id, 'end': last_id})

# 4. Find which chunks you need
required_tars = set()

# We iterate through your IDs and find the matching chunk for each.
# (This compares strings, which works for arXiv IDs like '2101.00123')
matched_count = 0

for target_id in my_ids:
    for chunk in chunks:
        # Check if the ID falls within the chunk's range
        if chunk['start'] <= target_id <= chunk['end']:
            required_tars.add(chunk['tar'])
            matched_count += 1
            break # Stop looking for this ID once found

print(f"Matched {matched_count} papers to source files.")
print(f"You need to download {len(required_tars)} tar files.")

# Optional: Save the list of tars to a file so you can use it in a downloader
with open("tars_to_download.txt", "w") as f:
    for tar in required_tars:
        f.write(tar + "\n")