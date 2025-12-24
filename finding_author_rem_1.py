"""
Finding Author Affiliations - Enhanced Parser (V2)

This script improves upon finding_author_rem.py by handling partially filled papers,
implementing affiliation inheritance, and handling collaboration authors.
"""

import pandas as pd
import ast
import re
from collections import defaultdict
from tqdm import tqdm


# ========================
# CONFIG
# ========================

CSV_PATH = "test_filled.csv"  # Start from the previously filled version
OUTPUT_CSV_PATH = "test_filled_2.csv"
LATEX_FILES = ["latex_affiliations_output.txt", "latex_affiliations_output_2.txt"]
REPORT_PATH = "missing_affiliations_report.txt"


# ========================
# PARSING UTILITIES
# ========================

def clean_latex_text(text):
    """Remove LaTeX commands and clean text."""
    if not text:
        return ""
    # Remove common LaTeX commands
    text = re.sub(r'\\[a-zA-Z]+\{([^}]*)\}', r'\1', text)  # \cmd{text} -> text
    text = re.sub(r'\\[a-zA-Z]+\[[^\]]*\]\{([^}]*)\}', r'\1', text)  # \cmd[opt]{text} -> text
    text = re.sub(r'\\[a-zA-Z]+', '', text)  # Remove remaining commands
    text = re.sub(r'[{}]', '', text)  # Remove braces
    # Cleanup leftover symbols often found in affiliations
    text = text.replace('\\\\', ', ')
    text = text.replace('\\', '')
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    # Remove leading/trailing commas or semicolons
    text = re.sub(r'^[\s,;.]+|[\s,;.]+$', '', text)
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
    
    author_pattern = r'\\author(?:\[[^\]]*\])?\{([^}]+)\}'
    affil_pattern = r'\\affiliation(?:\[[^\]]*\])?\{([^}]+)\}'
    
    lines = section.split('\n')
    current_author = None
    current_affiliations = []
    
    for line in lines:
        # Check for author
        author_match = re.search(author_pattern, line)
        if author_match:
            if current_author and current_affiliations:
                author_affiliations[current_author] = current_affiliations
            
            current_author = extract_name_from_author(author_match.group(1))
            current_affiliations = []
        
        # Check for affiliation
        affil_match = re.search(affil_pattern, line)
        if affil_match and current_author:
            affil = clean_latex_text(affil_match.group(1))
            if affil:
                current_affiliations.append(affil)
    
    if current_author and current_affiliations:
        author_affiliations[current_author] = current_affiliations
    
    return author_affiliations


def parse_jcap_style(section):
    """
    Parse JCAP style: \author[a,b]{Name} with \affiliation[a]{...}
    """
    author_affiliations = {}
    affiliation_map = {}
    
    affil_pattern = r'\\affiliation\[([^\]]+)\]\{([^}]+)\}'
    for match in re.finditer(affil_pattern, section):
        key = match.group(1).strip()
        affil = clean_latex_text(match.group(2))
        affiliation_map[key] = affil
    
    author_pattern = r'\\author\[([^\]]*)\]\{([^}]+)\}'
    for match in re.finditer(author_pattern, section):
        keys_str = match.group(1)
        author_raw = match.group(2)
        author = extract_name_from_author(author_raw)
        if not author: continue
        
        keys = [k.strip() for k in keys_str.split(',') if k.strip()]
        affiliations = [affiliation_map[key] for key in keys if key in affiliation_map]
        
        if affiliations:
            author_affiliations[author] = affiliations
    
    return author_affiliations


def parse_institute_style(section):
    """
    Parse RevTeX/institute style with superscripts or \inst{} markers.
    """
    author_affiliations = {}
    
    # Some papers have a single \institute{...} that applies to all
    institute_pattern = r'\\institute\{([^}]+(?:\{[^}]*\}[^}]*)*)\}'
    inst_match = re.search(institute_pattern, section)
    
    if inst_match:
        inst_text = inst_match.group(1)
        institutes = re.split(r'\\and|\\n\s*\n', inst_text)
        affil_list = [clean_latex_text(i) for i in institutes if clean_latex_text(i)]
        
        author_pattern = r'\\author\{([^}]+)\}'
        for match in re.finditer(author_pattern, section):
            author = extract_name_from_author(match.group(1))
            if author and affil_list:
                author_affiliations[author] = affil_list
    
    return author_affiliations


def parse_affil_style(section):
    """
    Parse \affil{} style.
    """
    author_affiliations = {}
    author_pattern = r'\\author\{([^}]+)\}'
    affil_pattern = r'\\affil(?:iation)?\{([^}]+)\}'
    
    authors = [extract_name_from_author(m.group(1)) for m in re.finditer(author_pattern, section)]
    affiliations = [clean_latex_text(m.group(1)) for m in re.finditer(affil_pattern, section)]
    
    if authors and affiliations:
        for author in authors:
            author_affiliations[author] = affiliations
            
    return author_affiliations


def parse_latex_section(section):
    """Try various strategies."""
    # Strategy 1: Multi-author blocks \author{A, B, C}
    # If we see \author{...} with commas inside, it might be a list
    author_list_match = re.search(r'\\author\{([^,]+(?:,[^,]+)+)\}', section)
    if author_list_match:
        names = [extract_name_from_author(n) for n in author_list_match.group(1).split(',')]
        affil_match = re.search(r'\\affil(?:iation)?\{([^}]+)\}', section) or re.search(r'\\affiliation\{([^}]+)\}', section)
        if affil_match:
            affil = clean_latex_text(affil_match.group(1))
            return {n: [affil] for n in names if n}

    for parser in [parse_aastex_style, parse_jcap_style, parse_institute_style, parse_affil_style]:
        res = parser(section)
        if res: return res
    return {}


def parse_latex_output_file(filepath):
    paper_affiliations = {}
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return {}
    
    paper_sections = re.split(r'-{70,}', content)
    current_title = None
    for section in paper_sections:
        title_match = re.search(r'PAPER:\s*(.+?)(?:\n|$)', section)
        if title_match:
            current_title = title_match.group(1).strip()
            continue
        if 'AFFILIATION SECTION:' in section or (current_title and 'ERROR:' not in section):
            if current_title:
                author_affils = parse_latex_section(section)
                if author_affils:
                    paper_affiliations[current_title] = author_affils
                current_title = None
    return paper_affiliations


def normalize_name(name):
    if not name: return ""
    name = name.lower()
    name = re.sub(r'^(dr\.?|prof\.?|mr\.?|ms\.?|mrs\.?)\s+', '', name)
    name = re.sub(r'[.,;:\-\'"`]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def get_name_parts(name):
    parts = normalize_name(name).split()
    if not parts: return [], ""
    if len(parts) == 1: return [], parts[0]
    return parts[:-1], parts[-1]


def names_match(name1, name2):
    if not name1 or not name2: return False
    n1, n2 = normalize_name(name1), normalize_name(name2)
    if n1 == n2: return True
    
    first1, last1 = get_name_parts(name1)
    first2, last2 = get_name_parts(name2)
    if last1 != last2: return False
    
    # Handle initials matching full names
    if not first1 or not first2: return True
    
    # Check for direct overlap in initials
    initials1 = set(p[0] for p in first1 if p)
    initials2 = set(p[0] for p in first2 if p)
    if initials1 & initials2: return True
    
    return False


_preprocessed_papers = None
_common_words = {'the', 'a', 'an', 'of', 'in', 'on', 'for', 'and', 'with', 'from', 'to', 'by', 'its', 'their', 'using', 'study', 'new', 'based'}

def preprocess_paper_affiliations(all_paper_affiliations):
    preprocessed = {}
    for latex_title, author_affils in all_paper_affiliations.items():
        title_words = set(normalize_name(latex_title).split()) - _common_words
        if title_words:
            preprocessed[latex_title] = {'words': title_words, 'authors': author_affils}
    return preprocessed


def match_author_to_affiliations(author_name, paper_title, all_paper_affiliations, preprocessed=None):
    global _preprocessed_papers
    if preprocessed is None:
        if _preprocessed_papers is None:
            _preprocessed_papers = preprocess_paper_affiliations(all_paper_affiliations)
        preprocessed = _preprocessed_papers
    
    title_words1 = set(normalize_name(paper_title).split()) - _common_words
    if not title_words1: return None
    
    min_overlap = min(3, len(title_words1))
    for latex_title, data in preprocessed.items():
        if len(title_words1 & data['words']) >= min_overlap:
            for latex_author, affiliations in data['authors'].items():
                if names_match(author_name, latex_author):
                    return affiliations
    return None


def main():
    print("=" * 60)
    print("Affiliation Filler V2 - Logic Refinement")
    print("=" * 60)
    
    df = pd.read_csv(CSV_PATH)
    all_paper_affiliations = {}
    for f in LATEX_FILES:
        all_paper_affiliations.update(parse_latex_output_file(f))
    
    print(f"Loaded {len(df)} papers and {len(all_paper_affiliations)} LaTeX matches.")
    
    def has_none(x):
        try:
            return any(a is None for a in ast.literal_eval(str(x)))
        except:
            return True

    missing_indices = df[df['affiliations'].apply(has_none)].index.tolist()
    print(f"Processing {len(missing_indices)} papers.")

    for idx in tqdm(missing_indices):
        row = df.loc[idx]
        try:
            authors = ast.literal_eval(str(row['authors']))
            affiliations = ast.literal_eval(str(row['affiliations']))
        except:
            continue
        
        while len(affiliations) < len(authors):
            affiliations.append(None)
            
        any_filled = False
        for i, (author, affil) in enumerate(zip(authors, affiliations)):
            if affil is None:
                new_affs = match_author_to_affiliations(author, row['title'], all_paper_affiliations)
                if new_affs:
                    affiliations[i] = "; ".join(new_affs) if isinstance(new_affs, list) else new_affs
                    any_filled = True
                # Handle Collaboration
                elif "collaboration" in author.lower() or "team" in author.lower():
                    # Pick the most common affiliation of the team if any
                    valid_affs = [a for a in affiliations if a is not None]
                    if valid_affs:
                        affiliations[i] = valid_affs[0]
                        any_filled = True

        # Inheritance: If some authors are missing but others have the SAME affiliation, 
        # it's likely they all share it (common in astronomy papers).
        valid_affs = [a for a in affiliations if a is not None]
        if None in affiliations and len(set(valid_affs)) == 1:
            shared_aff = valid_affs[0]
            affiliations = [shared_aff if a is None else a for a in affiliations]
            any_filled = True

        df.at[idx, 'affiliations'] = str(affiliations)

    df.to_csv(OUTPUT_CSV_PATH, index=False)
    
    # Report Update
    total_missing = 0
    remaining_papers = 0
    for idx, row in df.iterrows():
        try:
            affs = ast.literal_eval(str(row['affiliations']))
            misses = sum(1 for a in affs if a is None)
            if misses > 0:
                total_missing += misses
                remaining_papers += 1
        except:
            pass

    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write("=== MISSING AFFILIATIONS REPORT (V2) ===\n\n")
        f.write(f"Papers with missing: {remaining_papers}\n")
        f.write(f"Remaining null authors: {total_missing}\n")
    
    print(f"Done. Saved to {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    main()
