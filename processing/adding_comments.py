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
        print(f"[{index + 1}/{len(data)}] Skipping {arxiv_id}, comments already present.")
        continue

    url = base_url.format(arxiv_id)
    print(f"[{index + 1}/{len(data)}] Fetching {arxiv_id}...")
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            soup = bs4.BeautifulSoup(response.content, 'html.parser')
            
            # --- Strategy 1: Look for "Comments:" label specifically (Most reliable across layouts) ---
            # Finds <td class="tablecell label">Comments:</td> and gets the next sibling
            comments_label = soup.find('td', class_='tablecell label', string='Comments:')
            if comments_label:
                comments_value = comments_label.find_next_sibling('td', class_='tablecell arx-comment')
                # Sometimes the class isn't 'arx-comment', just the next td
                if not comments_value:
                     comments_value = comments_label.find_next_sibling('td')
                
                if comments_value:
                    data.at[index, 'comments'] = comments_value.text.strip()

            # --- Strategy 2: Look for "Journal-ref:" label ---
            journal_label = soup.find('td', class_='tablecell label', string='Journal-ref:')
            if journal_label:
                journal_value = journal_label.find_next_sibling('td', class_='tablecell jref')
                if not journal_value:
                     journal_value = journal_label.find_next_sibling('td')
                
                if journal_value:
                    data.at[index, 'journals'] = journal_value.text.strip()
            
            # --- Strategy 3: Fallback for newer CSS (div-based) layout ---
            if pd.isna(data.at[index, 'comments']):
               div_comments_label = soup.find('span', class_='descriptor', string='Comments:')
               if div_comments_label and div_comments_label.parent:
                   data.at[index, 'comments'] = div_comments_label.parent.text.replace('Comments:', '', 1).strip()

            if pd.isna(data.at[index, 'journals']):
               div_journal_label = soup.find('span', class_='descriptor', string='Journal-ref:')
               if div_journal_label and div_journal_label.parent:
                   data.at[index, 'journals'] = div_journal_label.parent.text.replace('Journal-ref:', '', 1).strip()
                   
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
