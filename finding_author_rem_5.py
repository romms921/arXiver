"""
Finding Author Affiliations - Enhanced Parser (V6 - Elsarticle)

Handles:
1. Elsarticle style: multiple \author[label] and \address[label] tags.
2. Label list mapping: \author[label1,label2]{...}.
3. Cleaning of Elsevier-specific tags like \corref, \cortext.
4. Robust fallback to previous styles.
"""

import pandas as pd
import ast
import re
from collections import defaultdict
from tqdm import tqdm


# ========================
# CONFIG
# ========================

CSV_PATH = "test_filled_5.csv"
OUTPUT_CSV_PATH = "test_filled_6.csv"
LATEX_FILES = ["latex_filtered_1.txt", "latex_filtered_2.txt"]
REPORT_PATH = "missing_affiliations_report.txt"


# ========================
# PARSING UTILITIES
# ========================

def clean_latex_text(text):
    if not text: return ""
    # Remove metadata and formatting
    text = re.sub(r'\\email\{[^}]*\}', '', text)
    text = re.sub(r'\\thanks\{[^}]*\}', '', text)
    text = re.sub(r'\\fnmsep', '', text)
    text = re.sub(r'\\vspace\{[^}]*\}', '', text)
    text = re.sub(r'\\corref\{[^}]*\}', '', text)
    text = re.sub(r'\\corref\[[^\]]*\]', '', text)
    
    # Recursive brace removal for simple formatting commands
    while True:
        new_text = re.sub(r'\\[a-zA-Z]+\{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}', r'\1', text)
        if new_text == text: break
        text = new_text
    
    text = re.sub(r'\\[a-zA-Z]+', ' ', text)
    text = re.sub(r'[{}]', ' ', text)
    text = text.replace('\\\\', ', ')
    text = text.replace('\\', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^[\s,;.]+|[\s,;.]+$', '', text)
    return text


def extract_name_from_author(author_str):
    if not author_str: return ""
    # Strip mapping markers to get the base name
    author_str = re.sub(r'\\inst\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\inst\[[^\]]*\]', '', author_str)
    author_str = re.sub(r'\\orcidlink\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\orcid\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\footnote\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\corref\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\$[\^]*\{?[\w, \-]*\}?\$', '', author_str) # Superscripts
    name = clean_latex_text(author_str)
    return name.strip()


def parse_elsarticle_style(section):
    """
    Handles elsarticle pattern: \author[label1,label2]{Name} and \address[label]{Affil}
    """
    author_affiliations = {}
    label_map = {}
    
    # 1. Collect Address Mappings
    # Matches: \address[label]{...}
    addr_matches = re.finditer(r'\\address\[([^\]]+)\]\{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}', section)
    for match in addr_matches:
        label = match.group(1).strip()
        affil = clean_latex_text(match.group(2))
        if label and affil: label_map[label] = affil
    
    # 2. Collect Authors and Map
    # Matches: \author[label]{Name}
    author_matches = re.finditer(r'\\author\[([^\]]+)\]\{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}', section)
    found_any = False
    for match in author_matches:
        label_str = match.group(1)
        name = extract_name_from_author(match.group(2))
        if not name: continue
        
        found_any = True
        labels = [l.strip() for l in label_str.split(',') if l.strip()]
        affs = [label_map[l] for l in labels if l in label_map]
        if affs: author_affiliations[name] = affs

    return author_affiliations


def parse_robust_mapping_style(section):
    author_affiliations = {}
    label_map = {}
    
    # 1. Collect Affiliation Mappings
    affil_matches = re.finditer(r'\\(?:affiliation|institute)\{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}', section)
    implicit_idx = 1
    
    for match in affil_matches:
        content = match.group(1)
        blocks = re.split(r'\\and|\\\\|\\n\s*\n', content)
        for b in blocks:
            b = b.strip()
            if not b: continue
            label_match = re.search(r'\$[\^]*\{?([\w, \-]+)\}?\$', b) or \
                          re.search(r'\\inst\{([\w, \-]+)\}', b)
            if label_match:
                label = label_match.group(1).strip()
                text = b.replace(label_match.group(0), '')
                affil = clean_latex_text(text)
                if affil: label_map[label] = affil
            else:
                affil = clean_latex_text(b)
                if affil:
                    label_map[str(implicit_idx)] = affil
                    implicit_idx += 1

    # 2. Collect Authors and Map
    author_tags = re.finditer(r'\\author(?:\[[^\]]*\])?\{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}', section)
    for tag in author_tags:
        content = tag.group(1)
        authors_raw = re.split(r'\\and|(?<!\{),(?![^}]*\})', content)
        for raw in authors_raw:
            name = extract_name_from_author(raw)
            if not name: continue
            indices = []
            inst_matches = re.finditer(r'\\inst\{([\w, \-]+)\}', raw)
            for m in inst_matches:
                indices.extend([i.strip() for i in m.group(1).split(',')])
            super_matches = re.finditer(r'\$[\^]*\{?([\w, \-]+)\}?\$', raw)
            for m in super_matches:
                indices.extend([i.strip() for i in m.group(1).split(',')])
            if indices:
                affs = [label_map[i] for i in indices if i in label_map]
                if affs: author_affiliations[name] = affs
            elif label_map:
                if len(label_map) == 1:
                    author_affiliations[name] = list(label_map.values())
                elif "1" in label_map:
                    author_affiliations[name] = [label_map["1"]]
    return author_affiliations


def parse_latex_section(section):
    # Try elsarticle specific parser first
    res = parse_elsarticle_style(section)
    if res: return res
    
    # Robust multi-author parser
    res = parse_robust_mapping_style(section)
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
    print("Affiliation Filler V6 - Elsevier & Elsarticle Style")
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
        f.write("=== MISSING AFFILIATIONS REPORT (V6) ===\n\n")
        f.write(f"Total papers: {total_papers}\n")
        f.write(f"Fully filled papers: {fully_filled_count}\n")
        f.write(f"Partially filled papers: {partially_filled_count}\n")
        f.write(f"Remaining incomplete indices ({len(incomplete_indices)}):\n")
        f.write(str(incomplete_indices))
        f.write("\n")
    
    print(f"Done. Results in {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    main()
