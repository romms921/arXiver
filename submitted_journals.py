import pandas as pd
import requests
import bs4
import time
import re
import os

data = pd.read_csv('2025_Data_missing.csv')

# Initialize new columns
data['comments'] = None
data['journals'] = None

base_url = "https://arxiv.org/list/astro-ph/2025-{:02d}?show=2000"

check = [1344, 1287, 1585, 1467, 1386, 1478, 1816, 1304, 1860, 1889, 1551, 1367]
months = range(1, 13)


for month in months:
    month_str = f"2025-{month:02d}"
    papers_to_process = check[month-1]
    url = base_url.format(month)
    
    print(f"[{month_str}] Fetching list (Targeting {papers_to_process} pure papers)...")
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(f"  Failed! Status code: {response.status_code}")
        
        soup = bs4.BeautifulSoup(response.content, 'html.parser')

        # Iterate over all papers on the page
        for dd in soup.find_all('dd'):
            # Extract title
            title_div = dd.find('div', class_='list-title')
            if title_div:
                # Clean title: remove "Title:" prefix and extra whitespace
                page_title = title_div.text.replace('Title:', '', 1).strip()
                
                # Check if title matches any in data
                matches = data['title'] == page_title
                
                if matches.any():
                    # Extract Comments
                    comments_div = dd.find('div', class_='list-comments')
                    if comments_div:
                        comments = comments_div.text.replace('Comments:', '', 1).strip()
                        data.loc[matches, 'comments'] = comments
                    
                    # Extract Journal-ref
                    journal_div = dd.find('div', class_='list-journal-ref')
                    if journal_div:
                        journal = journal_div.text.replace('Journal-ref:', '', 1).strip()
                        data.loc[matches, 'journals'] = journal

    except Exception as e:
        print(f"Error processing {month_str}: {e}")

    time.sleep(15)

# Save the updated data
data.to_csv('2025_Data_missing.csv', index=False)
