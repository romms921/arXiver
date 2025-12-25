import requests
from bs4 import BeautifulSoup
import time

url = "https://arxiv.org/html/2501.13056v1"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

try:
    print(f"Fetching {url}...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Dump the 'ltx_authors' section and any potential affiliation sections
    authors_section = soup.find(class_='ltx_authors')
    
    with open('debug_output.html', 'w', encoding='utf-8') as f:
        if authors_section:
            f.write("<!-- AUTHORS SECTION -->\n")
            f.write(authors_section.prettify())
            f.write("\n\n")
        else:
            f.write("<!-- NO LTX_AUTHORS FOUND -->\n")
            
        # Look for other potential classes mentioned in prompt
        # ltx_role_affiliation / ltx_role_address / ltx_personname
        
        f.write("<!-- ALL LTX_ROLE_AFFILIATION -->\n")
        for tag in soup.find_all(class_='ltx_role_affiliation'):
            f.write(tag.prettify() + "\n")

        f.write("\n<!-- ALL LTX_ROLE_ADDRESS -->\n")
        for tag in soup.find_all(class_='ltx_role_address'):
            f.write(tag.prettify() + "\n")
            
        f.write("\n<!-- ALL LTX_CONTACT -->\n")
        for tag in soup.find_all(class_='ltx_contact'):
            f.write(tag.prettify() + "\n")

    print("Done. Saved to debug_output.html")

except Exception as e:
    print(f"Error: {e}")
