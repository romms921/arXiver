import fitz
import requests
import io
import json
import google.generativeai as genai
import os
import dotenv

dotenv.load_dotenv()

# --- CONFIGURATION ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)

def download_arxiv_pdf(url):
    if not url: return None
    if not url.startswith(('http://', 'https://')): url = 'https://' + url
    if "arxiv.org" in url:
        if "/abs/" in url: url = url.replace("/abs/", "/pdf/")
        if not url.endswith(".pdf"): url += ".pdf"
    
    headers = {'User-Agent': 'AffiliationScraper/1.0'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return io.BytesIO(response.content)
    except Exception as e:
        print(f"Download Error: {e}")
        return None

def extract_text_from_stream(stream):
    try:
        doc = fitz.open(stream=stream, filetype="pdf")
        text = ""
        for i in range(min(2, len(doc))):
            text += doc[i].get_text()
        doc.close()
        return text
    except Exception as e:
        return f"Extraction Error: {e}"

def suggest_affiliations(paper_text, author_list):
    model = genai.GenerativeModel('gemini-2.0-flash')
    prompt = f"""
    Extract author affiliations from this text. 
    Match each author from the list below to their correct affiliation.
    Return ONLY a JSON object: {{"Author Name": "Affiliation String"}}
    
    AUTHORS: {author_list}
    TEXT: {paper_text}
    """
    try:
        response = model.generate_content(
            prompt, 
            generation_config={"response_mime_type": "application/json"}
        )
        print(f"AI Response Received: {response.text}") # CHECK YOUR TERMINAL FOR THIS
        return json.loads(response.text)
    except Exception as e:
        print(f"AI Error: {e}")
        return {"error": str(e)}