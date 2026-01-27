import json
from typing import List, Optional
from pydantic import BaseModel
from ollama import chat
from tqdm import tqdm

# --- CONFIGURATION ---
INPUT_FILE = "papers_data.txt"
OUTPUT_FILE = "extracted_affiliations.json"
MODEL = "llama3.2" 

# --- DATA STRUCTURES ---
class Author(BaseModel):
    name: str
    affiliations: List[str]
    email: Optional[str] = None

class PaperData(BaseModel):
    title: str
    authors: List[Author]

# --- PARSER WITH SKIP LOGIC ---
def paper_generator(file_path):
    """
    Yields only VALID papers containing LaTeX.
    Skips papers with 'ERROR: Failed to download'.
    """
    current_title = None
    current_latex = []
    
    # State flags
    is_recording = False  # Are we currently capturing text?
    is_error = False      # Did we hit an error for this paper?

    skipped_count = 0

    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            stripped = line.strip()

            # 1. NEW PAPER DETECTED
            if line.startswith("PAPER:"):
                # Yield previous paper ONLY if it was valid and not an error
                if current_title and current_latex and not is_error:
                    yield current_title, "".join(current_latex)
                
                # Reset for new paper
                current_title = line.replace("PAPER:", "").strip()
                current_latex = []
                is_recording = False
                is_error = False
                continue

            # 2. IGNORE DASHES
            if set(stripped) == {'-'}:
                continue

            # 3. DETECT ERROR (IMMEDIATE SKIP)
            if line.startswith("ERROR:"):
                is_error = True
                is_recording = False # Stop recording
                skipped_count += 1
                continue

            # 4. DETECT VALID CONTENT START
            if "AFFILIATION SECTION:" in line:
                is_recording = True
                continue

            # 5. CAPTURE CONTENT
            if is_recording and not is_error:
                current_latex.append(line)

        # Yield the very last paper if valid
        if current_title and current_latex and not is_error:
            yield current_title, "".join(current_latex)
            
    print(f"\n[Info] Skipped {skipped_count} papers due to download errors.")

# --- LLM INFERENCE ---
def extract_from_latex(title, latex_content):
    # Truncate to first 4000 chars to save speed/memory
    truncated_latex = latex_content[:4000]

    prompt = f"""
    Analyze the following LaTeX header to extract Author Affiliations.
    
    RULES:
    1. Extract all authors and their specific universities/institutes.
    2. Resolve cross-references: If you see `\\author[1]` and `\\affil[1]`, link them.
    3. Return valid JSON only.
    
    Paper Title: {title}
    
    LaTeX Content:
    ```latex
    {truncated_latex}
    ```
    """

    try:
        response = chat(
            model=MODEL,
            messages=[{'role': 'user', 'content': prompt}],
            format=PaperData.model_json_schema(),
            options={'temperature': 0} 
        )
        return json.loads(response.message.content)
    except Exception as e:
        return {"title": title, "authors": [], "error": str(e)}

# --- MAIN LOOP ---
def main():
    results = []
    
    # 1. Quick Count for Progress Bar
    print("Scanning file to count papers...")
    total_entries = 0
    with open(INPUT_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if line.startswith("PAPER:"):
                total_entries += 1
    print(f"Found {total_entries} total entries (including errors).")

    # 2. Process
    generator = paper_generator(INPUT_FILE)
    
    # We use total_entries for the bar, though it will finish 'early' 
    # because the generator skips the error ones invisibly.
    for title, latex in tqdm(generator, total=total_entries):
        
        data = extract_from_latex(title, latex)
        results.append(data)

        # Autosave every 50 VALID papers
        if len(results) % 50 == 0:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)

    # 3. Final Save
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
        
    print(f"Done. Processed {len(results)} valid papers.")

if __name__ == "__main__":
    main()