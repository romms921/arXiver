import json
import time
import os
from typing import List, Optional
from pydantic import BaseModel
from tqdm import tqdm
from openai import OpenAI

# --- CONFIGURATION ---
# 1. PASTE YOUR DEEPSEEK API KEY HERE
API_KEY = ""

# 2. FILE PATHS
INPUT_FILE = r"C:\Users\Rommulus\Documents\Astronomy\arXiver\latex_affiliations_output.txt"
OUTPUT_FILE = "extracted_affiliations.json"

# --- SETUP CLIENT (DEEPSEEK) ---
client = OpenAI(
    api_key=API_KEY, 
    base_url="https://api.deepseek.com"  # Connects to DeepSeek servers
)

# --- DATA STRUCTURES ---
class Author(BaseModel):
    name: str
    affiliations: List[str]
    email: Optional[str] = None

class PaperData(BaseModel):
    title: str
    authors: List[Author]

# --- PARSER ---
def paper_generator(file_path):
    current_title = None
    current_latex = []
    is_recording = False
    is_error = False

    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            stripped = line.strip()
            if line.startswith("PAPER:"):
                if current_title and current_latex and not is_error:
                    yield current_title, "".join(current_latex)
                current_title = line.replace("PAPER:", "").strip()
                current_latex = []
                is_recording = False
                is_error = False
                continue
            if set(stripped) == {'-'}: continue
            if line.startswith("ERROR:"):
                is_error = True
                is_recording = False
                continue
            if "AFFILIATION SECTION:" in line:
                is_recording = True
                continue
            if is_recording and not is_error:
                current_latex.append(line)
        if current_title and current_latex and not is_error:
            yield current_title, "".join(current_latex)

# --- DEEPSEEK INFERENCE ---
def extract_from_latex(title, latex_content):
    # DeepSeek V3 has a 64k context window, handling large papers easily
    truncated_latex = latex_content[:15000]

    prompt = f"""
    Extract Author Affiliations from the LaTeX header below into JSON.

    CRITICAL RULES:
    1. **Multiple Affiliations:** If an author has multiple markers (e.g., `^1,2` or `\\affil{{1,2}}`), you MUST add both institutions to their list.
    2. **Resolve References:** Match the numbers/symbols to the correct institution text.
    3. **JSON Only:** Return ONLY valid JSON, no markdown formatting like ```json.
    
    Target Structure:
    {{
      "title": "{title}",
      "authors": [
        {{ "name": "Author Name", "affiliations": ["Univ A", "Univ B"], "email": "opt" }}
      ]
    }}

    LaTeX Content:
    {truncated_latex}
    """

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",  # This is DeepSeek-V3 (Fast & Smart)
            messages=[
                {"role": "system", "content": "You are a helpful assistant that outputs strict JSON."},
                {"role": "user", "content": prompt},
            ],
            response_format={ 'type': 'json_object' }, # Enforces JSON
            temperature=0,
            stream=False
        )
        
        content = response.choices[0].message.content
        return json.loads(content)

    except Exception as e:
        print(f"Error on '{title}': {e}")
        # Basic rate limit handling just in case
        time.sleep(2) 
        return {"title": title, "authors": [], "error": str(e)}

# --- MAIN ---
def main():
    results = []
    
    if not os.path.exists(INPUT_FILE):
        print(f"Error: File not found at {INPUT_FILE}")
        return

    print("Counting papers...")
    total_entries = sum(1 for line in open(INPUT_FILE, 'r', encoding='utf-8', errors='ignore') if line.startswith("PAPER:"))
    
    print(f"Processing {total_entries} papers using DeepSeek API...")
    generator = paper_generator(INPUT_FILE)
    
    for title, latex in tqdm(generator, total=total_entries):
        data = extract_from_latex(title, latex)
        results.append(data)

        # Save frequently
        if len(results) % 10 == 0:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
    
    # Final Save
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    print("Done.")

if __name__ == "__main__":
    main()