"""
Finding Author Affiliations - Enhanced Parser (V3 - Incremental)

Focuses on remaining edge cases (suffixes, middle names, initials) and uses filtered LaTeX data.
"""

import pandas as pd
import ast
import re
from collections import defaultdict
from tqdm import tqdm


# ========================
# CONFIG
# ========================

CSV_PATH = "test_filled_2.csv"
OUTPUT_CSV_PATH = "test_filled_3.csv"
# Using filtered files for speed
LATEX_FILES = ["latex_filtered_1.txt", "latex_filtered_2.txt"]
REPORT_PATH = "missing_affiliations_report.txt"


# ========================
# PARSING UTILITIES
# ========================

def clean_latex_text(text):
    """Remove LaTeX commands and clean text."""
    if not text:
        return ""
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
    author_str = re.sub(r'\[[\d\-X]+\]', '', author_str)
    author_str = re.sub(r'\\orcidlink\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\orcid\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\note\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\$[^$]*\$', '', author_str)
    author_str = re.sub(r'\\thanks\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\altaffiliation\{[^}]*\}', '', author_str)
    name = clean_latex_text(author_str)
    name = re.sub(r'^[\s,;.]+|[\s,;.]+$', '', name)
    return name.strip()


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


def parse_institute_style(section):
    author_affiliations = {}
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
    author_list_match = re.search(r'\\author\{([^,]+(?:,[^,]+)+)\}', section)
    if author_list_match:
        names = [extract_name_from_author(n) for n in author_list_match.group(1).split(',')]
        aff_m = re.search(r'\\affil(?:iation)?\{([^}]+)\}', section)
        if aff_m:
            aff = clean_latex_text(aff_m.group(1))
            return {n: [aff] for n in names if n}
    for parser in [parse_aastex_style, parse_jcap_style, parse_institute_style, parse_affil_style]:
        res = parser(section)
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
    # Remove common suffixes
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
    # At least one initial overlap
    initials1 = set(p[0] for p in first1 if p)
    initials2 = set(p[0] for p in first2 if p)
    if initials1 & initials2: return True
    return False


_common_words = {'the', 'a', 'an', 'of', 'in', 'on', 'for', 'and', 'with', 'from', 'to', 'by', 'its', 'their', 'using', 'study', 'new', 'based'}

def match_author_to_affiliations(author_name, paper_title, all_paper_affiliations, preprocessed):
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
    print("Affiliation Filler V3 - Incremental Improvements")
    print("=" * 60)
    
    df = pd.read_csv(CSV_PATH)
    all_paper_affiliations = {}
    for f in LATEX_FILES:
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
    print(f"Processing {len(missing_indices)} papers with filtered LaTeX data.")

    for idx in tqdm(missing_indices):
        row = df.loc[idx]
        try:
            authors = ast.literal_eval(str(row['authors']))
            affiliations = ast.literal_eval(str(row['affiliations']))
        except: continue
        while len(affiliations) < len(authors): affiliations.append(None)
        
        for i, (author, affil) in enumerate(zip(authors, affiliations)):
            if affil is None:
                new_affs = match_author_to_affiliations(author, row['title'], all_paper_affiliations, preprocessed)
                if new_affs:
                    affiliations[i] = "; ".join(new_affs) if isinstance(new_affs, list) else new_affs
                elif "collaboration" in author.lower() or "team" in author.lower():
                    valid = [a for a in affiliations if a]
                    if valid: affiliations[i] = valid[0]

        # Inheritance
        valid_affs = [a for a in affiliations if a]
        if None in affiliations and len(set(valid_affs)) == 1:
            shared = valid_affs[0]
            affiliations = [shared if a is None else a for a in affiliations]
        
        df.at[idx, 'affiliations'] = str(affiliations)

    df.to_csv(OUTPUT_CSV_PATH, index=False)
    
    # NEW Reporting Format
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
        f.write("=== MISSING AFFILIATIONS REPORT (V3) ===\n\n")
        f.write(f"Total papers: {total_papers}\n")
        f.write(f"Fully filled papers: {fully_filled_count}\n")
        f.write(f"Partially filled papers: {partially_filled_count}\n")
        f.write(f"Remaining incomplete indices ({len(incomplete_indices)}):\n")
        f.write(str(incomplete_indices))
        f.write("\n")
    
    print(f"Done. Results in {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    main()
