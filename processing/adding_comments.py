import pandas as pd
import requests
import bs4
import time

data = pd.read_csv('2025_Data_missing.csv')

# Initialize new columns if they don't exist
if 'comments' not in data.columns:
    data['comments'] = None
if 'journals' not in data.columns:
    data['journals'] = None

base_url = "https://arxiv.org/abs/{}"

for index, row in data.iterrows():
    # Extract ID from pdf_link (assumes format like http://arxiv.org/pdf/2501.00001v1)
    pdf_link = str(row['pdf_link'])
    arxiv_id = pdf_link.split('/')[-1].replace('.pdf', '')
    
    # Skip if comments already collected (optional optimization)
    if pd.notna(row['comments']):
        continue

    url = base_url.format(arxiv_id)
    print(f"[{index + 1}/{len(data)}] Fetching {arxiv_id}...")
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            soup = bs4.BeautifulSoup(response.content, 'html.parser')
            
            # Find the div that contains the metadata
            meta_div = soup.find('div', class_='metatable')
            
            if meta_div:
                # Extract Comments
                comments_td = meta_div.find('td', class_='tablecell comments')
                if comments_td:
                    data.at[index, 'comments'] = comments_td.text.strip()
                
                # Extract Journal-ref
                journal_td = meta_div.find('td', class_='tablecell journal-ref')
                if journal_td:
                    data.at[index, 'journals'] = journal_td.text.strip()
            
            # Fallback for newer arXiv layout where class names are slightly different
            # Looking for direct sibling classes often used in abstract pages
            if pd.isna(data.at[index, 'comments']):
               comments_label = soup.find('span', class_='descriptor', string='Comments:')
               if comments_label and comments_label.parent:
                   data.at[index, 'comments'] = comments_label.parent.text.replace('Comments:', '', 1).strip()

            if pd.isna(data.at[index, 'journals']):
               journal_label = soup.find('span', class_='descriptor', string='Journal-ref:')
               if journal_label and journal_label.parent:
                   data.at[index, 'journals'] = journal_label.parent.text.replace('Journal-ref:', '', 1).strip()
                   
        else:
            print(f"  Failed! Status code: {response.status_code}")

    except Exception as e:
        print(f"Error processing {arxiv_id}: {e}")

    # Be polite to the server
    time.sleep(1)

    # Periodic save
    if index % 50 == 0:
        data.to_csv('2025_Data_missing.csv', index=False)

# Final save
data.to_csv('2025_Data_missing.csv', index=False)
