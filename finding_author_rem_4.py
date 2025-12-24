"""
Finding Author Affiliations - Enhanced Parser (V5 - OpenJournal & Letters)

Handles:
1. Document classes with multiple \author and \affiliation tags (OpenJournal).
2. Lettered mappings (a, b, c) in superscripts/subscripts.
3. Nested LaTeX structures in mappings.
"""

import pandas as pd
import ast
import re
from collections import defaultdict
from tqdm import tqdm


# ========================
# CONFIG
# ========================

CSV_PATH = "test_filled_4.csv"
OUTPUT_CSV_PATH = "test_filled_5.csv"
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
    # Remove superscript wrappers but keep content for later mapping if needed, 
    # but here we just want the text for the actual affiliation.
    # Recursive brace removal
    while True:
        new_text = re.sub(r'\\[a-zA-Z]+\{((?:[^{}]|\{[^{}]*\})*)\}', r'\1', text)
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
    author_str = re.sub(r'\$[\^]*\{?[\w, \-]*\}?\$', '', author_str) # Superscripts (letters/numbers)
    name = clean_latex_text(author_str)
    return name.strip()


def parse_robust_mapping_style(section):
    """
    Unified parser for OpenJournal, A&A, EVN2024, etc.
    Collects ALL authors and ALL affiliation mappings in a section.
    """
    author_affiliations = {}
    label_map = {}
    
    # 1. Collect Affiliation Mappings
    # Handles: \affiliation{${}^a ...}, \institute{...}, \affiliation{...}
    # Look for both \affiliation and \institute tags
    affil_matches = re.finditer(r'\\(?:affiliation|institute)\{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}', section)
    implicit_idx = 1
    
    for match in affil_matches:
        content = match.group(1)
        # Split by blocks (\and, \\, or blank lines)
        blocks = re.split(r'\\and|\\\\|\\n\s*\n', content)
        for b in blocks:
            b = b.strip()
            if not b: continue
            
            # Detect label in block: ${}^a$, \inst{1}, ^1, etc.
            label_match = re.search(r'\$[\^]*\{?([\w, \-]+)\}?\$', b) or \
                          re.search(r'\\inst\{([\w, \-]+)\}', b)
            
            if label_match:
                label = label_match.group(1).strip()
                # Remove label from text
                text = b.replace(label_match.group(0), '')
                affil = clean_latex_text(text)
                if affil: label_map[label] = affil
            else:
                # No label, use implicit index
                affil = clean_latex_text(b)
                if affil:
                    label_map[str(implicit_idx)] = affil
                    implicit_idx += 1

    # 2. Collect Authors and Map
    # Handles multiple \author{...} tags
    author_tags = re.finditer(r'\\author(?:\[[^\]]*\])?\{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)\}', section)
    
    found_any = False
    for tag in author_tags:
        content = tag.group(1)
        # Split individual authors in tag
        authors_raw = re.split(r'\\and|(?<!\{),(?![^}]*\})', content)
        
        for raw in authors_raw:
            name = extract_name_from_author(raw)
            if not name: continue
            found_any = True
            
            # Find indices in \inst{a,b} or $^a$
            indices = []
            # Extract from \inst{...}
            inst_matches = re.finditer(r'\\inst\{([\w, \-]+)\}', raw)
            for m in inst_matches:
                indices.extend([i.strip() for i in m.group(1).split(',')])
            
            # Extract from superscripts $^a$ or $^1$ or $^{a,b}$
            super_matches = re.finditer(r'\$[\^]*\{?([\w, \-]+)\}?\$', raw)
            for m in super_matches:
                indices.extend([i.strip() for i in m.group(1).split(',')])
            
            if indices:
                affs = [label_map[i] for i in indices if i in label_map]
                if affs: author_affiliations[name] = affs
            elif label_map:
                # Fallback if only one affil exists or implicit mapping
                if len(label_map) == 1:
                    author_affiliations[name] = list(label_map.values())
                elif "1" in label_map:
                    author_affiliations[name] = [label_map["1"]]

    return author_affiliations


def parse_jcap_style(section):
    # Already partially covered by robust but keep as fallback
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
    # Try robust parser first as it handles multiple tags
    res = parse_robust_mapping_style(section)
    if res: return res
    # Fallbacks
    for parser in [parse_jcap_style]:
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
    print("Affiliation Filler V5 - OpenJournal & Lettered Mappings")
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
        f.write("=== MISSING AFFILIATIONS REPORT (V5) ===\n\n")
        f.write(f"Total papers: {total_papers}\n")
        f.write(f"Fully filled papers: {fully_filled_count}\n")
        f.write(f"Partially filled papers: {partially_filled_count}\n")
        f.write(f"Remaining incomplete indices ({len(incomplete_indices)}):\n")
        f.write(str(incomplete_indices))
        f.write("\n")
    
    print(f"Done. Results in {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    main()
