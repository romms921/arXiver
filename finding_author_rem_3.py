"""
Finding Author Affiliations - Enhanced Parser (V4 - Robust Document Classes)

Focuses on complex document classes (evn2024, aa, mnras, etc.) 
handling numbered/indexed affiliations (inst, superscripts).
"""

import pandas as pd
import ast
import re
from collections import defaultdict
from tqdm import tqdm


# ========================
# CONFIG
# ========================

CSV_PATH = "test_filled_3.csv"
OUTPUT_CSV_PATH = "test_filled_4.csv"
LATEX_FILES = ["latex_filtered_1.txt", "latex_filtered_2.txt"]
REPORT_PATH = "missing_affiliations_report.txt"


# ========================
# PARSING UTILITIES
# ========================

def clean_latex_text(text):
    if not text: return ""
    # Remove metadata commands
    text = re.sub(r'\\email\{[^}]*\}', '', text)
    text = re.sub(r'\\thanks\{[^}]*\}', '', text)
    text = re.sub(r'\\fnmsep', '', text)
    # Remove LaTeX commands and keep content
    text = re.sub(r'\\[a-zA-Z]+\{([^}]*)\}', r'\1', text)
    text = re.sub(r'\\[a-zA-Z]+\[[^\]]*\]\{([^}]*)\}', r'\1', text)
    text = re.sub(r'\\[a-zA-Z]+', '', text)
    text = re.sub(r'[{}]', '', text)
    text = text.replace('\\\\', ', ')
    text = text.replace('\\', '')
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^[\s,;.]+|[\s,;.]+$', '', text)
    return text


def extract_name_from_author(author_str):
    if not author_str: return ""
    # Remove markers that are NOT names
    author_str = re.sub(r'\\inst\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\inst\[[^\]]*\]', '', author_str)
    author_str = re.sub(r'\$[\^]*\{?[\d, \-]*\}?\$', '', author_str) # Superscripts
    author_str = re.sub(r'\\orcidlink\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\orcid\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\$[^$]*\$', '', author_str)
    author_str = re.sub(r'\\thanks\{[^}]*\}', '', author_str)
    name = clean_latex_text(author_str)
    return name.strip()


def parse_numbered_style(section):
    """Handles EVN2024, A&A and other \inst or superscript styles."""
    author_affiliations = {}
    
    # 1. Extract Institute Map
    inst_map = {}
    # Find \institute{...} or equivalent
    inst_match = re.search(r'\\institute\{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}', section)
    if inst_match:
        inst_content = inst_match.group(1)
        # Split by \and or \\ and clean
        parts = re.split(r'\\and|\\\\|\\n\s*\n', inst_content)
        idx = 1
        for p in parts:
            # Check if there's an explicit \inst{N} prefix in the institute line
            prefix_match = re.match(r'^\s*\\inst\{(\d+)\}', p)
            if prefix_match:
                key = prefix_match.group(1)
                affil = clean_latex_text(p[prefix_match.end():])
                if affil: inst_map[key] = affil
            else:
                affil = clean_latex_text(p)
                if affil:
                    inst_map[str(idx)] = affil
                    idx += 1
    
    # 2. Extract Authors and their Indices
    author_match = re.search(r'\\author(?:\[[^\]]*\])?\{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}', section)
    if author_match:
        author_content = author_match.group(1)
        # Split authors
        authors_raw = re.split(r'\\and|(?<!\{),(?![^}]*\})', author_content) # Split by \and or commas not in braces
        
        for raw in authors_raw:
            name = extract_name_from_author(raw)
            if not name: continue
            
            # Find indices in \inst{1,2} or $^1$ or $^{1,2}$
            indices = []
            inst_idx_match = re.search(r'\\inst\{([\d, \-]+)\}', raw)
            if inst_idx_match:
                indices.extend([i.strip() for i in inst_idx_match.group(1).split(',')])
            
            super_idx_match = re.search(r'\$\^\{?([\d, \-]+)\}?\$', raw)
            if super_idx_match:
                indices.extend([i.strip() for i in super_idx_match.group(1).split(',')])
            
            if indices:
                affs = [inst_map[i] for i in indices if i in inst_map]
                if affs: author_affiliations[name] = affs
            elif inst_map:
                # Fallback: if no indices but only one institute, assign it
                if len(inst_map) == 1:
                    author_affiliations[name] = list(inst_map.values())
                else:
                    # Or check for the "first" if order is implicit
                    author_affiliations[name] = [inst_map.get("1")] if "1" in inst_map else []

    return author_affiliations


def parse_aastex_style(section):
    author_affiliations = {}
    author_pattern = r'\\author(?:\[[^\]]*\])?\{([^}]+)\}'
    affil_pattern = r'\\affiliation(?:\[[^\]]*\])?\{([^}]+)\}'
    lines = section.split('\n')
    current_author = None
    current_affiliations = []
    for line in lines:
        author_match = re.search(author_pattern, line)
        if author_match:
            if current_author and current_affiliations:
                author_affiliations[current_author] = current_affiliations
            current_author = extract_name_from_author(author_match.group(1))
            current_affiliations = []
        affil_match = re.search(affil_pattern, line)
        if affil_match and current_author:
            affil = clean_latex_text(affil_match.group(1))
            if affil:
                current_affiliations.append(affil)
    if current_author and current_affiliations:
        author_affiliations[current_author] = current_affiliations
    return author_affiliations


def parse_jcap_style(section):
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
        author = extract_name_from_author(match.group(2))
        if not author: continue
        keys = [k.strip() for k in keys_str.split(',') if k.strip()]
        affs = [affiliation_map[k] for k in keys if k in affiliation_map]
        if affs: author_affiliations[author] = affs
    return author_affiliations


def parse_latex_section(section):
    # Try styles in order of complexity/specificity
    for parser in [parse_numbered_style, parse_jcap_style, parse_aastex_style]:
        res = parser(section)
        if res:
            # Filter out empty results
            res = {k: v for k, v in res.items() if v}
            if res: return res
    return {}


def parse_filtered_file(filepath):
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
    name = re.sub(r'\s+(jr\.?|sr\.?|iii|ii|iv)$', '', name)
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
    if not first1 or not first2: return True
    initials1 = set(p[0] for p in first1 if p)
    initials2 = set(p[0] for p in first2 if p)
    if initials1 & initials2: return True
    return False


_common_words = {'the', 'a', 'an', 'of', 'in', 'on', 'for', 'and', 'with', 'from', 'to', 'by', 'its', 'their', 'using', 'study', 'new', 'based'}

def match_author_to_affiliations(author_name, paper_title, preprocessed):
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
    print("Affiliation Filler V4 - Robust Document Classes")
    print("=" * 60)
    
    df = pd.read_csv(CSV_PATH)
    all_paper_affiliations = {}
    for f in LATEX_FILES:
        print(f"Parsing {f}...")
        all_paper_affiliations.update(parse_filtered_file(f))
    
    preprocessed = {}
    for t, a in all_paper_affiliations.items():
        words = set(normalize_name(t).split()) - _common_words
        if words: preprocessed[t] = {'words': words, 'authors': a}

    def is_missing(x):
        try:
            affs = ast.literal_eval(str(x))
            return isinstance(affs, list) and any(a is None for a in affs)
        except:
            return True

    missing_indices = df[df['affiliations'].apply(is_missing)].index.tolist()
    print(f"Processing {len(missing_indices)} papers with enhanced parsing.")

    for idx in tqdm(missing_indices):
        row = df.loc[idx]
        try:
            authors = ast.literal_eval(str(row['authors']))
            affiliations = ast.literal_eval(str(row['affiliations']))
        except: continue
        while len(affiliations) < len(authors): affiliations.append(None)
        
        changed = False
        for i, (author, affil) in enumerate(zip(authors, affiliations)):
            if affil is None:
                new_affs = match_author_to_affiliations(author, row['title'], preprocessed)
                if new_affs:
                    affiliations[i] = "; ".join(new_affs) if isinstance(new_affs, list) else new_affs
                    changed = True
                elif "collaboration" in author.lower() or "team" in author.lower():
                    valid = [a for a in affiliations if a]
                    if valid: 
                        affiliations[i] = valid[0]
                        changed = True

        # Inheritance
        valid_affs = [a for a in affiliations if a]
        if None in affiliations and len(set(valid_affs)) == 1:
            shared = valid_affs[0]
            affiliations = [shared if a is None else a for a in affiliations]
            changed = True
        
        if changed:
            df.at[idx, 'affiliations'] = str(affiliations)

    df.to_csv(OUTPUT_CSV_PATH, index=False)
    
    # Reporting
    fully_filled_count = 0
    partially_filled_count = 0
    incomplete_indices = []
    total_papers = len(df)

    for idx, row in df.iterrows():
        try:
            affs = ast.literal_eval(str(row['affiliations']))
            if not isinstance(affs, list):
                incomplete_indices.append(idx)
                continue
            null_count = sum(1 for a in affs if a is None)
            if null_count == 0:
                fully_filled_count += 1
            else:
                partially_filled_count += 1
                incomplete_indices.append(idx)
        except:
            incomplete_indices.append(idx)

    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write("=== MISSING AFFILIATIONS REPORT (V4) ===\n\n")
        f.write(f"Total papers: {total_papers}\n")
        f.write(f"Fully filled papers: {fully_filled_count}\n")
        f.write(f"Partially filled papers: {partially_filled_count}\n")
        f.write(f"Remaining incomplete indices ({len(incomplete_indices)}):\n")
        f.write(str(incomplete_indices))
        f.write("\n")
    
    print(f"Done. Results in {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    main()
