"""
Gemini API-based Author Affiliation Extractor
Uses Gemini for batch processing of papers with structured JSON output.
"""

import json
import logging
import os
import random
import re
import shutil
import tarfile
import time
from datetime import datetime
from typing import List, Optional

import dotenv
import pandas as pd
import requests
from google import genai
from pydantic import BaseModel, Field
from tqdm import tqdm


# =======================
# PYDANTIC MODELS FOR STRUCTURED OUTPUT
# =======================
class Author(BaseModel):
    """Model for a single author with affiliations."""
    name: Optional[str] = Field(default="", description="Full name of the author.")
    affiliations: List[str] = Field(default_factory=list, description="List of affiliations for this author.")
    countries: List[str] = Field(default_factory=list, description="List of countries associated with the affiliations.")
    
    class Config:
        # Allow extra fields and be lenient with validation
        extra = "ignore"
        validate_assignment = False


class Paper(BaseModel):
    """Model for a single paper's extracted data."""
    arxiv_id: str = Field(description="The arXiv ID of the paper.")
    authors: List[Author] = Field(default_factory=list, description="List of authors with their affiliations.")
    first_author_countries: List[str] = Field(default_factory=list, description="Countries of the first author.")
    
    class Config:
        # Allow extra fields and be lenient with validation
        extra = "ignore"
        validate_assignment = False


class BatchResponse(BaseModel):
    """Model for the batch response containing multiple papers."""
    papers: List[Paper] = Field(description="List of papers with extracted author and affiliation data.")

# =======================
# CONFIGURATION
# =======================
# Model recommendations (based on available models):
# - "gemini-2.5-flash": Best balance - fast, accurate, excellent JSON compliance (RECOMMENDED)
# - "gemini-2.5-pro": Most accurate but slower, best for complex extractions
# - "gemini-3-flash-preview": Newest model, may have best JSON compliance (preview)
# - "gemini-3-pro-preview": Most powerful, best accuracy (preview, slower)
# - "gemini-2.5-flash-lite": Fastest but less accurate
# - "gemini-2.0-flash": Stable, reliable, good JSON output
MODEL_ID = "gemini-3-pro-preview"  # Recommended: best balance of speed and JSON compliance
CSV_PATH = "2025_Data.csv"
OUTPUT_CSV_PATH = "gemini_affil_output.csv"

# Local directory containing arXiv LaTeX folders
ARXIV_LATEX_DIR = r"C:\Users\hetan\Desktop\DesktopStuff\arxiv_latex\arXiver"
OUTPUT_JSON = "gemini_output.json"

# Batch configuration
BATCH_SIZE = 15 # Number of papers per batch
MAX_LATEX_CHARS = 25000  # Max chars of LaTeX per paper

# Rate limiting for free tier: 15 requests per minute
RATE_LIMIT_SECONDS = 5  # ~4 seconds between requests
MAX_RETRIES = 5  # Increased retries for 503 errors
RETRY_DELAY = 60  # seconds to wait on quota error
SERVICE_UNAVAILABLE_BASE_DELAY = 30  # Base delay for 503 errors (exponential backoff)
MAX_SERVICE_UNAVAILABLE_DELAY = 300  # Max 5 minutes wait for 503 errors

# NOTE: 503 errors ("model is overloaded") are SERVER-SIDE issues, not context memory issues.
# The Gemini API service itself is overloaded. Each API call using generate_content() is 
# stateless - there's no conversation history or model context memory maintained between 
# requests. Each call is completely independent. The solution is to retry with exponential
# backoff and increase wait times between requests to reduce load on the API.

# =======================
# LOGGING SETUP
# =======================
def setup_logging():
    """Configure logging with both file and console handlers."""
    log_filename = f"gemini_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Create logger
    logger = logging.getLogger("GeminiExtractor")
    logger.setLevel(logging.DEBUG)
    
    # Clear existing handlers
    logger.handlers = []
    
    # File handler - detailed logging
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)
    
    # Console handler - info level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(levelname)-8s | %(message)s')
    console_handler.setFormatter(console_format)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# =======================
# INIT GEMINI CLIENT
# =======================
dotenv.load_dotenv()

client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))


# =======================
# UTILITY FUNCTIONS
# =======================
def setup_directories():
    """Verify the local LaTeX directory exists."""
    if not os.path.exists(ARXIV_LATEX_DIR):
        logger.error(f"LaTeX directory not found: {ARXIV_LATEX_DIR}")
        raise FileNotFoundError(f"LaTeX directory not found: {ARXIV_LATEX_DIR}")
    logger.info(f"Using LaTeX directory: {ARXIV_LATEX_DIR}")



def get_eprint_url(pdf_link):
    """Converts a PDF link to an e-print source link."""
    if not isinstance(pdf_link, str):
        return None, None

    match = re.search(r"arxiv.org/pdf/([\d\.]+)", pdf_link)
    if not match:
        match = re.search(r"arxiv.org/abs/([\d\.]+)", pdf_link)

    if match:
        arxiv_id = match.group(1)
        return f"https://arxiv.org/e-print/{arxiv_id}", arxiv_id
    return None, None


# Note: download_source function removed - now reading from local folders instead

def read_latex_from_folder(arxiv_id):
    """
    Reads all .tex files from a local folder and combines them.
    The folder name should match the arxiv_id.
    Returns combined LaTeX content or None if folder not found.
    """
    folder_path = os.path.join(ARXIV_LATEX_DIR, arxiv_id)
    
    if not os.path.exists(folder_path):
        logger.warning(f"Folder not found for {arxiv_id}: {folder_path}")
        return None
    
    if not os.path.isdir(folder_path):
        logger.warning(f"Path exists but is not a directory: {folder_path}")
        return None
    
    # Find all .tex files in the folder (including subdirectories)
    tex_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".tex") or file.endswith(".ltx"):
                tex_files.append(os.path.join(root, file))
    
    if not tex_files:
        logger.warning(f"No .tex files found in folder: {folder_path}")
        return None
    
    # Sort by size (largest first) to prioritize main files
    tex_files.sort(key=lambda x: os.path.getsize(x), reverse=True)
    
    logger.debug(f"Found {len(tex_files)} .tex files for {arxiv_id}")
    
    # Combine all .tex files into one content string
    combined_content = []
    total_chars = 0
    
    for tex_file in tex_files:
        try:
            with open(tex_file, "r", encoding='utf-8', errors="replace") as f:
                content = f.read()
                # Add a separator comment to distinguish files
                rel_path = os.path.relpath(tex_file, folder_path)
                combined_content.append(f"\n% ===== File: {rel_path} =====\n")
                combined_content.append(content)
                total_chars += len(content)
        except Exception as e:
            logger.warning(f"Error reading {tex_file}: {e}")
            continue
    
    if combined_content:
        full_content = "\n".join(combined_content)
        logger.info(f"Combined {len(tex_files)} .tex files for {arxiv_id} ({total_chars} chars total)")
        return full_content
    
    return None


def cleanup(arxiv_id):
    """No cleanup needed when reading from local folders."""
    pass


# =======================
# GEMINI API FUNCTIONS
# =======================
def build_batch_prompt(papers_data):
    """
    Build a prompt for a batch of papers.
    papers_data: list of dicts with 'arxiv_id', 'title', 'latex_text'
    """
    system_prompt = """You are a helpful assistant that extracts structured author and affiliation data from LaTeX documents.

For each paper provided:
1. Extract all author names in order
2. For each author, extract their affiliations (some authors have multiple)
3. For each affiliation, identify the country
4. Identify the first author's country/countries
5. If you cannot find the information, use empty strings "" for names and empty arrays [] for lists.

CRITICAL JSON REQUIREMENTS:
- Return ONLY valid JSON - no markdown code blocks, no explanations
- Escape all backslashes properly: use \\\\ for literal backslashes in text
- Use double quotes for all strings
- Convert LaTeX commands to plain text (remove \\ commands, convert to readable text)
- Ensure all special characters are properly escaped
- If a field is missing, use empty string "" or empty array [] as appropriate
"""

    # Build the papers section
    papers_section = ""
    for i, paper in enumerate(papers_data, 1):
        # Just truncate the LaTeX - no filtering
        truncated_latex = paper['latex_text'][:MAX_LATEX_CHARS]
        
        papers_section += f"""
--- PAPER {i} ---
ArXiv ID: {paper['arxiv_id']}
Title: {paper.get('title', 'Unknown')}

LaTeX Content:
{truncated_latex}

"""

    user_prompt = f"""
Extract author and affiliation information from these papers:

{papers_section}

Return ONLY a valid JSON object (no markdown, no explanations) with this exact structure:
{{
  "papers": [
    {{
      "arxiv_id": "the_arxiv_id",
      "authors": [
        {{
          "name": "Author Full Name",
          "affiliations": ["Institution 1", "Institution 2"],
          "countries": ["Country1", "Country2"]
        }}
      ],
      "first_author_countries": ["Country"]
    }}
  ]
}}

IMPORTANT: 
- Escape all backslashes in text (use \\\\ for literal backslash)
- Use empty string "" if name is missing
- Use empty array [] if list is missing
- Convert LaTeX to plain text (remove \\ commands)
"""
    
    return system_prompt, user_prompt


def clean_json_response(response_text: str) -> str:
    """
    Clean up JSON response to fix common escape character issues.
    LaTeX content often contains backslashes that break JSON parsing.
    Uses multiple strategies to fix invalid escape sequences.
    """
    if not response_text:
        return response_text
    
    # Remove markdown code blocks if present
    response_text = response_text.strip()
    if response_text.startswith("```json"):
        response_text = response_text[7:].strip()
    elif response_text.startswith("```"):
        response_text = response_text[3:].strip()
    if response_text.endswith("```"):
        response_text = response_text[:-3].strip()
    
    # First, try parsing as-is
    try:
        json.loads(response_text)
        return response_text  # It's already valid
    except json.JSONDecodeError:
        pass
    
    # Strategy 1: Fix invalid escape sequences in string values
    # This is the most common issue - backslashes in LaTeX commands
    def fix_escapes_in_strings(text):
        """Fix invalid escape sequences inside JSON string values."""
        result = []
        in_string = False
        escape_next = False
        i = 0
        
        while i < len(text):
            char = text[i]
            
            # Track string boundaries (handle escaped quotes)
            if char == '"' and not escape_next:
                in_string = not in_string
                result.append(char)
                escape_next = False
            elif char == '\\' and in_string:
                # We're inside a string and found a backslash
                if i + 1 < len(text):
                    next_char = text[i + 1]
                    # Valid JSON escape sequences
                    valid_escapes = {'"', '\\', '/', 'b', 'f', 'n', 'r', 't', 'u'}
                    
                    if next_char in valid_escapes:
                        # Valid escape, keep as-is
                        result.append(char)
                        result.append(next_char)
                        i += 1  # Skip next char as we've already added it
                    elif next_char == 'u' and i + 5 < len(text):
                        # Unicode escape \uXXXX
                        result.append(char)
                        result.append(next_char)
                        i += 1
                    else:
                        # Invalid escape - double the backslash
                        result.append('\\\\')
                        # Don't skip next char, it might be part of the content
                else:
                    # Backslash at end of string - escape it
                    result.append('\\\\')
                escape_next = False
            else:
                result.append(char)
                escape_next = (char == '\\' and not in_string)
            
            i += 1
        
        return ''.join(result)
    
    # Apply fix
    fixed_text = fix_escapes_in_strings(response_text)
    
    # Try parsing again
    try:
        json.loads(fixed_text)
        return fixed_text
    except json.JSONDecodeError as e:
        logger.debug(f"First fix attempt failed: {e}")
    
    # Strategy 2: More aggressive - replace all single backslashes in strings
    # (except valid escapes we already handled)
    def aggressive_fix(text):
        """More aggressive fix for stubborn escape issues."""
        result = []
        in_string = False
        i = 0
        
        while i < len(text):
            char = text[i]
            
            if char == '"' and (i == 0 or text[i-1] != '\\' or (i > 1 and text[i-2] == '\\')):
                # Check if quote is escaped by counting backslashes
                backslash_count = 0
                j = i - 1
                while j >= 0 and text[j] == '\\':
                    backslash_count += 1
                    j -= 1
                
                if backslash_count % 2 == 0:
                    in_string = not in_string
                result.append(char)
            elif char == '\\' and in_string:
                if i + 1 < len(text):
                    next_char = text[i + 1]
                    if next_char in '"\\/bfnrt' or (next_char == 'u' and i + 5 < len(text)):
                        # Valid escape
                        result.append(char)
                    else:
                        # Invalid - double it
                        result.append('\\\\')
                else:
                    result.append('\\\\')
            else:
                result.append(char)
            i += 1
        
        return ''.join(result)
    
    fixed_text = aggressive_fix(fixed_text)
    
    # Strategy 3: Remove control characters that might break JSON
    # Replace problematic control chars (except newlines/tabs in strings)
    # Remove null bytes and other problematic control chars outside of valid escapes
    fixed_text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', fixed_text)
    
    return fixed_text


def query_gemini_batch(papers_data: list, retry_count: int = 0) -> Optional[BatchResponse]:
    """
    Send a batch of papers to Gemini and get structured extraction results.
    Uses Pydantic schema for reliable JSON parsing.
    Returns BatchResponse or None on failure.
    """
    if not papers_data:
        return None
    
    # Log batch info
    paper_names = [f"{p['arxiv_id']}: {p.get('title', 'No title')[:50]}..." for p in papers_data]
    logger.info(f"Processing batch of {len(papers_data)} papers:")
    for name in paper_names:
        logger.info(f"  - {name}")
    
    system_prompt, user_prompt = build_batch_prompt(papers_data)
    
    try:
        # Each API call is stateless - no conversation history or context memory.
        # The 'contents' parameter is just the current prompt, not a chat history.
        # This ensures complete isolation between batches.
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=user_prompt,  # Single prompt, no history
            config={
                "system_instruction": system_prompt,
                "temperature": 0,
                "response_mime_type": "application/json",
                "response_json_schema": BatchResponse.model_json_schema(),
            }
        )
        
        response_text = response.text
        logger.debug(f"Received raw response ({len(response_text)} chars)")
        
        # Clean the response to fix escape character issues
        cleaned_response = clean_json_response(response_text)
        
        # Try parsing with Pydantic - multiple strategies
        parse_attempts = [
            ("Pydantic validate_json", lambda: BatchResponse.model_validate_json(cleaned_response)),
            ("Manual JSON + Pydantic validate", lambda: BatchResponse.model_validate(json.loads(cleaned_response))),
        ]
        
        for attempt_name, parse_func in parse_attempts:
            try:
                batch_response = parse_func()
                logger.info(f"✅ Successfully parsed {len(batch_response.papers)} papers using {attempt_name}")
                return batch_response
            except json.JSONDecodeError as json_err:
                logger.debug(f"{attempt_name} failed (JSON error): {json_err}")
                # Try to extract JSON from the response if it's wrapped
                try:
                    # Look for JSON object boundaries
                    start_idx = cleaned_response.find('{')
                    end_idx = cleaned_response.rfind('}')
                    if start_idx >= 0 and end_idx > start_idx:
                        extracted_json = cleaned_response[start_idx:end_idx+1]
                        batch_response = BatchResponse.model_validate(json.loads(extracted_json))
                        logger.info(f"✅ Successfully parsed {len(batch_response.papers)} papers (extracted JSON)")
                        return batch_response
                except:
                    pass
            except Exception as parse_error:
                logger.debug(f"{attempt_name} failed: {parse_error}")
                # Try to fix common Pydantic validation errors
                try:
                    # Parse JSON manually first
                    data = json.loads(cleaned_response)
                    # Fix common issues in the data structure
                    if "papers" in data:
                        for paper in data["papers"]:
                            # Ensure required fields exist
                            if "arxiv_id" not in paper:
                                paper["arxiv_id"] = ""
                            if "authors" not in paper:
                                paper["authors"] = []
                            if "first_author_countries" not in paper:
                                paper["first_author_countries"] = []
                            # Fix authors
                            for author in paper.get("authors", []):
                                if "name" not in author or author["name"] is None:
                                    author["name"] = ""
                                if "affiliations" not in author:
                                    author["affiliations"] = []
                                if "countries" not in author:
                                    author["countries"] = []
                                # Ensure lists are actually lists
                                if not isinstance(author.get("affiliations"), list):
                                    author["affiliations"] = []
                                if not isinstance(author.get("countries"), list):
                                    author["countries"] = []
                        
                        batch_response = BatchResponse.model_validate(data)
                        logger.info(f"✅ Successfully parsed {len(batch_response.papers)} papers (with data fixes)")
                        return batch_response
                except Exception as fix_error:
                    logger.debug(f"Data fix attempt failed: {fix_error}")
        
        # All parsing attempts failed
        logger.error(f"All parsing attempts failed. Response length: {len(cleaned_response)}")
        logger.debug(f"Cleaned response (first 2000 chars): {cleaned_response[:2000]}")
        logger.debug(f"Cleaned response (last 500 chars): {cleaned_response[-500:]}")
        
        # Try to save problematic response for debugging
        try:
            debug_file = f"failed_response_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(debug_file, "w", encoding='utf-8') as f:
                f.write(cleaned_response)
            logger.warning(f"Saved failed response to {debug_file} for debugging")
        except:
            pass
        
        return None
        
    except Exception as e:
        error_msg = str(e).lower()
        error_code = None
        
        # Extract error code if available
        if hasattr(e, 'status_code'):
            error_code = e.status_code
        elif hasattr(e, 'response') and hasattr(e.response, 'status_code'):
            error_code = e.response.status_code
        
        # Handle rate limit errors (429)
        if "quota" in error_msg or "rate" in error_msg or "429" in str(error_code) or error_code == 429:
            if retry_count < MAX_RETRIES:
                logger.warning(f"Rate limit hit (429). Waiting {RETRY_DELAY}s before retry {retry_count + 1}/{MAX_RETRIES}")
                logger.debug(f"Error details: {e}")
                time.sleep(RETRY_DELAY)
                return query_gemini_batch(papers_data, retry_count + 1)
            else:
                logger.error(f"Max retries exceeded for rate limit")
                return None
        
        # Handle service unavailable errors (503) with exponential backoff
        elif "503" in str(error_code) or error_code == 503 or "unavailable" in error_msg or "overloaded" in error_msg:
            if retry_count < MAX_RETRIES:
                # Exponential backoff: 30s, 60s, 120s, 240s, capped at 300s
                delay = min(SERVICE_UNAVAILABLE_BASE_DELAY * (2 ** retry_count), MAX_SERVICE_UNAVAILABLE_DELAY)
                logger.warning(f"Service unavailable (503) - model overloaded. Waiting {delay}s before retry {retry_count + 1}/{MAX_RETRIES}")
                logger.info(f"This is a server-side issue - the Gemini API is temporarily overloaded. Retrying with exponential backoff...")
                time.sleep(delay)
                return query_gemini_batch(papers_data, retry_count + 1)
            else:
                logger.error(f"Max retries exceeded for service unavailable error. The API may be experiencing high load.")
                logger.info(f"Consider reducing BATCH_SIZE or increasing wait times between requests.")
                return None
        
        # Handle other server errors (500, 502, 504) with exponential backoff
        elif error_code in [500, 502, 504]:
            if retry_count < MAX_RETRIES:
                delay = min(SERVICE_UNAVAILABLE_BASE_DELAY * (2 ** retry_count), MAX_SERVICE_UNAVAILABLE_DELAY)
                logger.warning(f"Server error ({error_code}). Waiting {delay}s before retry {retry_count + 1}/{MAX_RETRIES}")
                time.sleep(delay)
                return query_gemini_batch(papers_data, retry_count + 1)
            else:
                logger.error(f"Max retries exceeded for server error {error_code}")
                return None
        
        # Other errors - log and fail
        else:
            logger.error(f"Gemini API Error: {e}")
            logger.debug(f"Error code: {error_code}, Error message: {error_msg}")
            import traceback
            logger.debug(traceback.format_exc())
            return None


def format_paper_for_csv(paper: Paper) -> tuple:
    """Format a single Paper model for CSV output."""
    if not paper:
        return "[]", "[]", "[]", ""

    authors = []
    affiliations = []
    countries = []

    for author in paper.authors:
        authors.append(author.name)
        
        # Clean affiliations
        clean_affs = [aff.replace("\n", " ").strip() for aff in author.affiliations]
        affiliations.append("; ".join(clean_affs))
        
        # Clean countries
        clean_cnts = [cnt.replace("\n", " ").strip() for cnt in author.countries]
        countries.append("; ".join(clean_cnts))

    # First author countries
    first_author_country = "; ".join(
        [cnt.replace("\n", " ").strip() for cnt in paper.first_author_countries]
    )

    return (
        json.dumps(authors),
        json.dumps(affiliations),
        json.dumps(countries),
        first_author_country,
    )


# =======================
# BATCH PROCESSING
# =======================


def collect_batch(df, indices, link_col):
    """
    Collect papers for a batch by reading LaTeX from local folders.
    Returns list of paper data dicts.
    """
    batch_data = []
    
    for i, index in enumerate(indices):
        row = df.loc[index]
        pdf_link = row[link_col]
        title = row.get('title', 'Unknown')
        
        # Extract arxiv_id from PDF link
        _, arxiv_id = get_eprint_url(pdf_link)
        
        if not arxiv_id:
            logger.warning(f"[{index}] No valid arXiv ID extracted from {pdf_link}")
            continue
        
        # Read LaTeX from local folder
        read_start = time.time()
        latex_text = read_latex_from_folder(arxiv_id)
        read_end = time.time()
        
        if latex_text:
            batch_data.append({
                'original_index': index,
                'arxiv_id': arxiv_id,
                'title': title,
                'latex_text': latex_text
            })
            logger.info(f"✅ [{index}] Collected {arxiv_id}: {title[:50]}... ({read_end - read_start:.2f}s)")
        else:
            logger.warning(f"[{index}] No LaTeX text found for {arxiv_id}")
    
    logger.info(f"📦 Batch collection complete: {len(batch_data)}/{len(indices)} papers collected")
    return batch_data


def process_batch_results(batch_response: BatchResponse, batch_data: list, output_csv: str) -> int:
    """
    Process and save batch results to CSV and JSON.
    batch_response: Pydantic BatchResponse model
    batch_data: list of dicts with original_index, arxiv_id, title, latex_text
    """
    if not batch_response or not batch_response.papers:
        logger.error("Invalid or empty batch response")
        return 0
    
    saved_count = 0
    papers_map = {p['arxiv_id']: p for p in batch_data}
    
    for paper in batch_response.papers:
        arxiv_id = paper.arxiv_id
        
        # Find matching paper data
        original_data = papers_map.get(arxiv_id)
        if not original_data:
            # Try to match by position if ID doesn't match exactly
            logger.warning(f"Could not match arxiv_id: {arxiv_id}")
            continue
        
        original_index = original_data['original_index']
        
        # Log to JSON (serialize Pydantic model to dict)
        with open(OUTPUT_JSON, "a", encoding='utf-8') as f:
            entry = {
                "index": original_index,
                "arxiv_id": arxiv_id,
                "data": paper.model_dump()
            }
            f.write(json.dumps(entry) + "\n")
        
        # Format for CSV
        authors, affils, countries, first_country = format_paper_for_csv(paper)
        
        # Append to CSV
        res_df = pd.DataFrame([{
            "original_index": original_index,
            "arxiv_id": arxiv_id,
            "extracted_authors": authors,
            "extracted_affiliations": affils,
            "extracted_countries": countries,
            "first_author_country": first_country,
        }])
        res_df.to_csv(output_csv, mode="a", header=False, index=False)
        
        logger.info(f"[{original_index}] Saved result for {arxiv_id}")
        saved_count += 1
    
    return saved_count


def main():
    logger.info("=" * 60)
    logger.info("GEMINI AUTHOR AFFILIATION EXTRACTOR")
    logger.info(f"Model: {MODEL_ID}")
    logger.info(f"Batch Size: {BATCH_SIZE}")
    logger.info(f"Max LaTeX chars per paper: {MAX_LATEX_CHARS}")
    logger.info("=" * 60)
    
    setup_directories()
    
    # Read dataset
    logger.info(f"Reading dataset: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    logger.info(f"Total papers in dataset: {len(df)}")
    
    # Find link column
    link_col = None
    for col in df.columns:
        if "pdf_link" in col.lower():
            link_col = col
            break
    
    if not link_col:
        for col in df.columns:
            if len(df) > 0:
                val = str(df[col].iloc[0])
                if "arxiv.org" in val:
                    link_col = col
                    break
    
    if not link_col:
        logger.error("Could not find pdf_link column")
        return
    
    logger.info(f"Using link column: {link_col}")
    
    # Ensure output CSV has header if new
    if not os.path.exists(OUTPUT_CSV_PATH):
        pd.DataFrame(columns=[
            "original_index",
            "arxiv_id",
            "extracted_authors",
            "extracted_affiliations",
            "extracted_countries",
            "first_author_country",
        ]).to_csv(OUTPUT_CSV_PATH, index=False)
        logger.info(f"Created output CSV: {OUTPUT_CSV_PATH}")
    
    # Get already processed indices
    processed_indices = set()
    try:
        existing = pd.read_csv(OUTPUT_CSV_PATH)
        if "original_index" in existing.columns:
            processed_indices = set(existing["original_index"].tolist())
            logger.info(f"Found {len(processed_indices)} already processed entries")
    except:
        pass
    
    # Filter to unprocessed entries
    indices_to_process = [i for i in df.index if i not in processed_indices]
    
    if not indices_to_process:
        logger.info("No new entries to process.")
        return
    
    logger.info(f"Papers to process: {len(indices_to_process)}")
    
    # Process in batches
    total_saved = 0
    total_batches = (len(indices_to_process) + BATCH_SIZE - 1) // BATCH_SIZE
    
    pbar = tqdm(
        range(0, len(indices_to_process), BATCH_SIZE),
        total=total_batches,
        desc="Processing batches"
    )
    
    for batch_start in pbar:
        batch_end = min(batch_start + BATCH_SIZE, len(indices_to_process))
        batch_indices = indices_to_process[batch_start:batch_end]
        
        logger.info(f"\n{'='*40}")
        logger.info(f"BATCH {batch_start // BATCH_SIZE + 1}/{total_batches}")
        logger.info(f"Indices: {batch_indices}")
        logger.info(f"{'='*40}")
        
        # Print paper titles for sanity check
        print("\n" + "=" * 50)
        print(f"📚 BATCH {batch_start // BATCH_SIZE + 1}/{total_batches} - Papers:")
        for idx in batch_indices:
            title = df.loc[idx].get('title', 'Unknown')[:60]
            print(f"   [{idx}] {title}...")
        print("=" * 50 + "\n")
        
        # Collect batch data (read LaTeX from local folders)
        start_time = time.time()
        batch_data = collect_batch(df, batch_indices, link_col)
        
        if not batch_data:
            logger.warning("No valid papers in this batch, skipping...")
            continue
        
        # Query Gemini with batch
        logger.info(f"Sending batch of {len(batch_data)} papers to Gemini...")
        batch_response = query_gemini_batch(batch_data)
        
        if batch_response:
            saved = process_batch_results(batch_response, batch_data, OUTPUT_CSV_PATH)
            total_saved += saved
            logger.info(f"Batch complete: {saved}/{len(batch_data)} papers saved")
            pbar.set_postfix({"saved": total_saved})
        else:
            logger.error("Failed to get response for batch")
        
        # Rate limiting - increased wait time to reduce load on API
        elapsed = time.time() - start_time
        # Add extra buffer time to reduce chance of 503 errors
        # Add small random jitter (0-2 seconds) to avoid synchronized requests
        base_wait = RATE_LIMIT_SECONDS * 1.5  # 50% extra buffer
        jitter = random.uniform(0, 2)  # Random 0-2 seconds
        wait_time = max(0, base_wait + jitter - elapsed)
        if wait_time > 0:
            logger.debug(f"Rate limit: waiting {wait_time:.1f}s (with buffer and jitter to reduce API load)")
            time.sleep(wait_time)
    
    logger.info("=" * 60)
    logger.info(f"PROCESSING COMPLETE")
    logger.info(f"Total papers saved: {total_saved}")
    logger.info(f"Output CSV: {OUTPUT_CSV_PATH}")
    logger.info(f"Output JSON: {OUTPUT_JSON}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
