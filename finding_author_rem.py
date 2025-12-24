"""
Finding Author Affiliations - Enhanced Parser

This script parses LaTeX affiliation output files and fills in missing affiliations
in the test.csv file. It handles multiple LaTeX formatting styles for author/affiliation
mappings.
"""

import pandas as pd
import ast
import re
from collections import defaultdict
from tqdm import tqdm


# ========================
# CONFIG
# ========================

CSV_PATH = "test.csv"
OUTPUT_CSV_PATH = "test_filled.csv"
LATEX_FILES = ["latex_affiliations_output.txt", "latex_affiliations_output_2.txt"]
REPORT_PATH = "missing_affiliations_report.txt"


# ========================
# PARSING UTILITIES
# ========================

def clean_latex_text(text):
    """Remove LaTeX commands and clean text."""
    # Remove common LaTeX commands
    text = re.sub(r'\\[a-zA-Z]+\{([^}]*)\}', r'\1', text)  # \cmd{text} -> text
    text = re.sub(r'\\[a-zA-Z]+\[[^\]]*\]\{([^}]*)\}', r'\1', text)  # \cmd[opt]{text} -> text
    text = re.sub(r'\\[a-zA-Z]+', '', text)  # Remove remaining commands
    text = re.sub(r'[{}]', '', text)  # Remove braces
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    return text


def extract_name_from_author(author_str):
    """Extract clean author name from LaTeX author command."""
    # Handle \author[orcid]{Name} or \author{Name}
    # Remove ORCID patterns
    author_str = re.sub(r'\[[\d\-X]+\]', '', author_str)
    author_str = re.sub(r'\\orcidlink\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\orcid\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\note\{[^}]*\}', '', author_str)
    
    # Remove footnote markers
    author_str = re.sub(r'\$[^$]*\$', '', author_str)
    author_str = re.sub(r'\\thanks\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\altaffiliation\{[^}]*\}', '', author_str)
    
    # Clean remaining LaTeX
    name = clean_latex_text(author_str)
    
    # Remove leading/trailing punctuation
    name = re.sub(r'^[\s,;.]+|[\s,;.]+$', '', name)
    
    return name.strip()


def parse_aastex_style(section):
    """
    Parse AASTeX style: \author{Name} followed by \affiliation{...}
    Returns dict mapping author names to list of affiliations.
    """
    author_affiliations = {}
    
    # Find all author blocks
    # Pattern: \author with optional orcid in brackets, then name in braces
    author_pattern = r'\\author(?:\[[^\]]*\])?\{([^}]+)\}'
    affil_pattern = r'\\affiliation(?:\[[^\]]*\])?\{([^}]+)\}'
    
    lines = section.split('\n')
    current_author = None
    current_affiliations = []
    
    for line in lines:
        # Check for author
        author_match = re.search(author_pattern, line)
        if author_match:
            # Save previous author if exists
            if current_author and current_affiliations:
                author_affiliations[current_author] = current_affiliations
            
            current_author = extract_name_from_author(author_match.group(1))
            current_affiliations = []
        
        # Check for affiliation (applies to current author)
        affil_match = re.search(affil_pattern, line)
        if affil_match and current_author:
            affil = clean_latex_text(affil_match.group(1))
            if affil:
                current_affiliations.append(affil)
    
    # Save last author
    if current_author and current_affiliations:
        author_affiliations[current_author] = current_affiliations
    
    return author_affiliations


def parse_jcap_style(section):
    """
    Parse JCAP style: \author[a,b]{Name} with \affiliation[a]{...}
    Returns dict mapping author names to list of affiliations.
    """
    author_affiliations = {}
    affiliation_map = {}  # Maps keys (a, b, 1, 2, etc.) to affiliation text
    
    # First, extract all affiliations with their keys
    affil_pattern = r'\\affiliation\[([^\]]+)\]\{([^}]+)\}'
    for match in re.finditer(affil_pattern, section):
        key = match.group(1).strip()
        affil = clean_latex_text(match.group(2))
        affiliation_map[key] = affil
    
    # Then find authors and their affiliation keys
    author_pattern = r'\\author\[([^\]]*)\]\{([^}]+)\}'
    for match in re.finditer(author_pattern, section):
        keys_str = match.group(1)
        author_raw = match.group(2)
        
        author = extract_name_from_author(author_raw)
        if not author:
            continue
        
        # Parse affiliation keys (can be comma-separated)
        keys = [k.strip() for k in keys_str.split(',') if k.strip()]
        
        # Get affiliations for this author
        affiliations = []
        for key in keys:
            if key in affiliation_map:
                affiliations.append(affiliation_map[key])
        
        if affiliations:
            author_affiliations[author] = affiliations
    
    return author_affiliations


def parse_institute_style(section):
    """
    Parse RevTeX/institute style with superscripts or \inst{} markers.
    """
    author_affiliations = {}
    
    # Look for \institute{...} blocks
    institute_pattern = r'\\institute\{([^}]+(?:\{[^}]*\}[^}]*)*)\}'
    inst_match = re.search(institute_pattern, section)
    
    if inst_match:
        # Parse numbered institutes
        inst_text = inst_match.group(1)
        # Split by \and or newlines
        institutes = re.split(r'\\and|\n\s*\n', inst_text)
        affil_list = [clean_latex_text(i) for i in institutes if clean_latex_text(i)]
        
        # Find authors
        author_pattern = r'\\author\{([^}]+)\}'
        for match in re.finditer(author_pattern, section):
            author = extract_name_from_author(match.group(1))
            if author and affil_list:
                author_affiliations[author] = affil_list[:1]  # Assign first affiliation
    
    return author_affiliations


def parse_affil_style(section):
    """
    Parse \affil{} style (simpler format).
    """
    author_affiliations = {}
    
    # Pattern for \author{Name} 
    author_pattern = r'\\author\{([^}]+)\}'
    affil_pattern = r'\\affil(?:iation)?\{([^}]+)\}'
    
    authors = []
    affiliations = []
    
    for match in re.finditer(author_pattern, section):
        name = extract_name_from_author(match.group(1))
        if name:
            authors.append(name)
    
    for match in re.finditer(affil_pattern, section):
        affil = clean_latex_text(match.group(1))
        if affil:
            affiliations.append(affil)
    
    # If we found both, map them
    if authors and affiliations:
        for author in authors:
            author_affiliations[author] = affiliations[:1]  # Give first affiliation
    
    return author_affiliations


def parse_latex_section(section):
    """
    Parse a LaTeX section and extract author-affiliation mappings.
    Tries multiple parsing strategies.
    """
    result = {}
    
    # Try AASTeX style first (most common)
    result = parse_aastex_style(section)
    if result:
        return result
    
    # Try JCAP style
    result = parse_jcap_style(section)
    if result:
        return result
    
    # Try institute style
    result = parse_institute_style(section)
    if result:
        return result
    
    # Try simple affil style
    result = parse_affil_style(section)
    
    return result


def parse_latex_output_file(filepath):
    """
    Parse a LaTeX affiliations output file.
    Returns a dict mapping paper titles to author-affiliation dicts.
    """
    paper_affiliations = {}
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return {}
    
    # Split by paper delimiter
    paper_sections = re.split(r'-{70,}', content)
    
    current_title = None
    for section in paper_sections:
        # Check if this is a title section (starts with "PAPER:")
        title_match = re.search(r'PAPER:\s*(.+?)(?:\n|$)', section)
        if title_match:
            current_title = title_match.group(1).strip()
            continue
        
        # Check if this is an affiliation section
        if 'AFFILIATION SECTION:' in section or (current_title and 'ERROR:' not in section):
            if current_title:
                # Parse this section
                author_affils = parse_latex_section(section)
                if author_affils:
                    paper_affiliations[current_title] = author_affils
                current_title = None
    
    return paper_affiliations


def normalize_name(name):
    """Normalize a name for matching (lowercase, remove punctuation, handle initials)."""
    if not name:
        return ""
    
    # Convert to lowercase
    name = name.lower()
    
    # Remove common prefixes
    name = re.sub(r'^(dr\.?|prof\.?|mr\.?|ms\.?|mrs\.?)\s+', '', name)
    
    # Normalize spaces and punctuation
    name = re.sub(r'[.,;:\-\'"`]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


def get_name_parts(name):
    """Get first and last name parts."""
    parts = normalize_name(name).split()
    if len(parts) == 0:
        return [], ""
    if len(parts) == 1:
        return [], parts[0]
    
    # Last word is usually last name, rest are first/middle
    return parts[:-1], parts[-1]


def names_match(name1, name2):
    """
    Check if two names refer to the same person.
    Handles initials, different orderings, etc.
    """
    if not name1 or not name2:
        return False
    
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    
    # Exact match
    if n1 == n2:
        return True
    
    # Get name parts
    first1, last1 = get_name_parts(name1)
    first2, last2 = get_name_parts(name2)
    
    # Last names must match
    if last1 != last2:
        return False
    
    # If either has no first name parts, consider it a match based on last name
    if not first1 or not first2:
        return True
    
    # Check if first names/initials match
    # At least one first name initial should match
    initials1 = set(p[0] for p in first1 if p)
    initials2 = set(p[0] for p in first2 if p)
    
    if initials1 & initials2:  # If there's overlap in initials
        return True
    
    # Check if any full first name matches
    for f1 in first1:
        for f2 in first2:
            if f1 == f2:
                return True
            # One might be initial of the other
            if len(f1) == 1 and f2.startswith(f1):
                return True
            if len(f2) == 1 and f1.startswith(f2):
                return True
    
    return False


# Global cache for preprocessed paper data
_preprocessed_papers = None
_common_words = {'the', 'a', 'an', 'of', 'in', 'on', 'for', 'and', 'with', 'from', 'to', 'by', 'its', 'their', 'using', 'study', 'new', 'based'}

def preprocess_paper_affiliations(all_paper_affiliations):
    """
    Preprocess all paper affiliations for fast lookup.
    Returns dict with normalized title data for quick matching.
    """
    preprocessed = {}
    for latex_title, author_affils in all_paper_affiliations.items():
        # Precompute normalized title words
        title_words = set(normalize_name(latex_title).split()) - _common_words
        if title_words:
            preprocessed[latex_title] = {
                'words': title_words,
                'authors': author_affils
            }
    return preprocessed


def match_author_to_affiliations(author_name, paper_title, all_paper_affiliations, preprocessed=None):
    """
    Try to find affiliations for an author from the parsed LaTeX data.
    Uses preprocessed data for fast matching.
    """
    global _preprocessed_papers
    
    # Build preprocessed data if not provided
    if preprocessed is None:
        if _preprocessed_papers is None:
            _preprocessed_papers = preprocess_paper_affiliations(all_paper_affiliations)
        preprocessed = _preprocessed_papers
    
    # Precompute query title words
    title_words1 = set(normalize_name(paper_title).split()) - _common_words
    if not title_words1:
        return None
    
    min_overlap = min(3, len(title_words1))
    
    # Find matching paper
    for latex_title, data in preprocessed.items():
        title_words2 = data['words']
        
        if not title_words2:
            continue
        
        overlap = len(title_words1 & title_words2)
        if overlap >= min_overlap:
            # Found matching paper, now find the author
            for latex_author, affiliations in data['authors'].items():
                if names_match(author_name, latex_author):
                    return affiliations
    
    return None


def has_missing_affiliations(aff_val):
    """Check if affiliations value contains None values."""
    if pd.isna(aff_val):
        return True
    
    if isinstance(aff_val, str):
        try:
            aff_val = ast.literal_eval(aff_val)
        except:
            return True
    
    if isinstance(aff_val, list):
        return any(v is None for v in aff_val)
    
    return True


def main():
    print("=" * 60)
    print("Affiliation Filler - Parsing LaTeX output files")
    print("=" * 60)
    
    # Load CSV
    print(f"\nLoading: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    print(f"Total papers: {len(df)}")
    
    # Parse all LaTeX output files
    all_paper_affiliations = {}
    for latex_file in LATEX_FILES:
        print(f"\nParsing: {latex_file}")
        paper_affils = parse_latex_output_file(latex_file)
        print(f"  Found {len(paper_affils)} papers with parsed affiliations")
        all_paper_affiliations.update(paper_affils)
    
    print(f"\nTotal papers with parsed affiliations: {len(all_paper_affiliations)}")
    
    # Find papers with missing affiliations
    missing_mask = df['affiliations'].apply(has_missing_affiliations)
    missing_indices = df[missing_mask].index.tolist()
    print(f"Papers with missing affiliations: {len(missing_indices)}")
    
    # Process each paper with missing affiliations
    filled_count = 0
    partially_filled_count = 0
    still_missing_indices = []
    
    for idx in tqdm(missing_indices, desc="Filling affiliations"):
        row = df.loc[idx]
        title = row['title']
        
        # Get current authors and affiliations
        try:
            authors = ast.literal_eval(str(row['authors']))
        except:
            authors = []
        
        try:
            affiliations = ast.literal_eval(str(row['affiliations']))
        except:
            affiliations = [None] * len(authors) if authors else []
        
        if not authors:
            still_missing_indices.append(idx)
            continue
        
        # Ensure affiliations list matches authors length
        while len(affiliations) < len(authors):
            affiliations.append(None)
        
        # Try to fill in missing affiliations
        any_filled = False
        all_filled = True
        
        for i, (author, affil) in enumerate(zip(authors, affiliations)):
            if affil is None:
                # Try to find affiliation from LaTeX data
                new_affils = match_author_to_affiliations(author, title, all_paper_affiliations)
                if new_affils:
                    # Join multiple affiliations with semicolon
                    affiliations[i] = "; ".join(new_affils) if len(new_affils) > 1 else new_affils[0]
                    any_filled = True
                else:
                    all_filled = False
        
        # Update the dataframe
        df.at[idx, 'affiliations'] = str(affiliations)
        
        if any_filled and all_filled:
            filled_count += 1
        elif any_filled:
            partially_filled_count += 1
            still_missing_indices.append(idx)
        else:
            still_missing_indices.append(idx)
    
    # Save updated CSV
    print(f"\nSaving to: {OUTPUT_CSV_PATH}")
    df.to_csv(OUTPUT_CSV_PATH, index=False)
    
    # Calculate remaining missing statistics
    new_missing_mask = df['affiliations'].apply(has_missing_affiliations)
    remaining_missing = df[new_missing_mask].index.tolist()
    
    # Count remaining missing authors
    total_remaining_authors = 0
    for idx in remaining_missing:
        try:
            affiliations = ast.literal_eval(str(df.loc[idx, 'affiliations']))
            if isinstance(affiliations, list):
                total_remaining_authors += sum(1 for a in affiliations if a is None)
        except:
            pass
    
    # Update report
    print(f"\nUpdating report: {REPORT_PATH}")
    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write("=== MISSING AFFILIATIONS REPORT (UPDATED) ===\n\n")
        f.write(f"Original papers in dataset: {len(df)}\n")
        f.write(f"Original papers with missing affiliations: {len(missing_indices)}\n\n")
        f.write(f"Papers fully filled: {filled_count}\n")
        f.write(f"Papers partially filled: {partially_filled_count}\n")
        f.write(f"Papers still missing affiliations: {len(remaining_missing)}\n")
        f.write(f"Remaining authors with missing affiliations: {total_remaining_authors}\n\n")
        f.write("=== INDICES OF PAPERS STILL MISSING AFFILIATIONS ===\n")
        f.write(str(remaining_missing))
        f.write("\n")
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Papers fully filled: {filled_count}")
    print(f"Papers partially filled: {partially_filled_count}")
    print(f"Papers still missing affiliations: {len(remaining_missing)}")
    print(f"Remaining authors with missing affiliations: {total_remaining_authors}")
    print(f"\nOutput saved to: {OUTPUT_CSV_PATH}")
    print(f"Report saved to: {REPORT_PATH}")


if __name__ == "__main__":
    main()