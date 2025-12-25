"""
Finding Author Affiliations - Enhanced Parser (V9 - Parbox & Manual Formatting)

Handles:
1. \parbox{\textwidth} blocks for authors and affiliations.
2. Manual superscript mapping (e.g. $^{1}$) in parbox blocks.
3. Inherits all previous parser logic (Altaffil, Nested Braces, Elsevier, etc.).
"""

import pandas as pd
import ast
import re
from collections import defaultdict
from tqdm import tqdm

# ========================
# CONFIG
# ========================

CSV_PATH = "test_filled_8.csv"
OUTPUT_CSV_PATH = "test_filled_9.csv"
LATEX_FILES = ["latex_filtered_1.txt", "latex_filtered_2.txt"]
REPORT_PATH = "missing_affiliations_report.txt"


# ========================
# PARSING UTILITIES
# ========================

def extract_balanced_content(text, start_token):
    """Extracts content within balanced curly braces following start_token."""
    if not text: return None
    pattern = re.escape(start_token) + r'\s*\{'
    match = re.search(pattern, text)
    if not match: return None
    
    start_idx = match.end()
    stack = 1
    for i in range(start_idx, len(text)):
        if text[i] == '{':
            stack += 1
        elif text[i] == '}':
            stack -= 1
            if stack == 0:
                return text[start_idx:i]
    return None


def clean_latex_text(text):
    if not text: return ""
    text = re.sub(r'\\email\{[^}]*\}', '', text)
    text = re.sub(r'\\thanks\{[^}]*\}', '', text)
    text = re.sub(r'\\fnmsep', '', text)
    text = re.sub(r'\\vspace\{[^}]*\}', '', text)
    text = re.sub(r'\\corref\{[^}]*\}', '', text)
    
    for _ in range(4):
        new_text = re.sub(r'\\[a-zA-Z]+\{((?:[^{}]|\{[^{}]*\})*)\}', r'\1', text)
        if new_text == text: break
        text = new_text
    
    text = re.sub(r'\\[a-zA-Z]+', ' ', text)
    text = re.sub(r'[{}]', ' ', text)
    text = text.replace('\\\\', ', ')
    text = text.replace('\\', ' ')
    text = text.replace('$', '') 
    text = text.replace('~', ' ') # Handle non-breaking space
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^[\s,;.]+|[\s,;.]+$', '', text)
    return text


def extract_name_from_author(author_str):
    if not author_str: return ""
    author_str = re.sub(r'\\altaffilmark\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\inst\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\orcidlink\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\footnote\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\$[\^]*\{?[\w, \-$\star\dagger]*\}?\$', '', author_str)
    name = clean_latex_text(author_str)
    return name.strip()


def parse_parbox_style(section):
    """
    Handles \parbox{\textwidth}{...authors...} followed by \parbox{\textwidth}{...affiliations...}
    """
    author_affiliations = {}
    label_map = {}
    
    parbox_matches = list(re.finditer(r'\\parbox\{\\textwidth\}', section))
    if len(parbox_matches) < 2: return {}

    # Extract contents of all parboxes
    parbox_contents = []
    for match in parbox_matches:
        content = extract_balanced_content(section[match.start():], '\\parbox{\\textwidth}')
        if content: parbox_contents.append(content)
    
    if not parbox_contents: return {}

    # Heuristic: Affiliation block usually contains "University", "Department", "Institute" etc.
    # Author block usually has names.
    # Or strict ordering: 1st block authors, 2nd block affiliations.
    
    # Let's try to identify affiliation block by detecting standard address keywords
    affil_keywords = ['university', 'institute', 'department', 'school', 'laboratory', 'center', 'observatory']
    
    affil_block = None
    author_block = None
    
    for content in parbox_contents:
        lower_content = content.lower()
        if any(kw in lower_content for kw in affil_keywords) and '\\\\' in content:
            affil_block = content
        else:
            # If not identified as affiliation, assume authors if it has superscripts
            if '$^' in content or '\\thanks' in content:
                author_block = content
    
    # Fallback: Assume first is authors, second is affiliations if specific identification fails
    if not affil_block and len(parbox_contents) >= 2:
        affil_block = parbox_contents[-1]
        author_block = parbox_contents[0] # Simplistic assumption

    if not affil_block or not author_block: return {}

    # Parse Affiliations
    # Format: $^{1}$ Affiliation text \\
    # Split by \\ or newline
    lines = re.split(r'\\\\|\\par', affil_block)
    for line in lines:
        line = line.strip()
        if not line: continue
        
        # Match label: $^{1}$ or ${^1}$
        label_match = re.search(r'\$[\^]*\{?([\w, \-]+)\}?\$', line)
        if label_match:
            label = label_match.group(1).strip()
            affil_text = line.replace(label_match.group(0), '')
            affil = clean_latex_text(affil_text)
            if affil: label_map[label] = affil
            
    # Parse Authors
    # Format: Name$^{\orcid...}$,$^{1}$
    # Split authors likely by newlines in source or commas
    # The example has one author per line? No, looks like newlines in latex source.
    
    # Clean author block of some noise first?
    cleaned_author_block = re.sub(r'\\and', '\n', author_block)
    
    # Split by common delimiters
    # Authors might be separated by commas if not newlines
    # But names also have commas (Surname, Name). 
    # Let's try splitting by lines first.
    author_lines = cleaned_author_block.split('\n')
    names_raw = []
    for line in author_lines:
        if line.strip(): names_raw.append(line.strip())
        
    if len(names_raw) < 2: # Maybe they are comma separated
        names_raw = re.split(r'(?<!\{),(?![^}]*\})', cleaned_author_block)

    for raw in names_raw:
        if not raw.strip(): continue
        name = extract_name_from_author(raw)
        if not name: continue
        
        # Extract indices
        indices = []
        # Match $^{1}$ or $^{1,2}$
        super_matches = re.finditer(r'\$[\^]*\{?([\w, \-]+)\}?\$', raw)
        for m in super_matches:
            # Check if it looks like an ORCID (skip) or affiliation index
            # ORCID usually long, numbers. Affil index usually short numbers.
            content = m.group(1)
            # Remove orcid parts if mixed? The example has separate superscripts for orcid and affil.
            # J.~Callow$^{\orcidlink{...}}$,$^{1}$
            if 'orcid' in content.lower(): continue
            
            parts = [p.strip() for p in content.split(',')]
            indices.extend(parts)
        
        if indices:
            affs = [label_map[i] for i in indices if i in label_map]
            if affs: author_affiliations[name] = affs
    
    return author_affiliations


def parse_altaffil_style(section):
    author_affiliations = {}
    label_map = {}
    for match in re.finditer(r'\\altaffiltext', section):
        block_start = match.start()
        label = extract_balanced_content(section[block_start:], '\\altaffiltext')
        if label:
            label_match_str = '{' + label + '}'
            label_pos = section.find(label_match_str, block_start)
            if label_pos != -1:
                affil_start = label_pos + len(label_match_str)
                affil_search_str = section[affil_start:].lstrip()
                if affil_search_str.startswith('{'):
                    affil_content = extract_balanced_content('placeholder' + affil_search_str, 'placeholder')
                    if affil_content:
                        clean_label = label.strip().replace('$', '').replace('\\', '')
                        label_map[clean_label] = clean_latex_text(affil_content)

    for match in re.finditer(r'\\author', section):
        content = extract_balanced_content(section[match.start():], '\\author')
        if content:
            authors_raw = re.split(r'\\and|(?<!\{),(?![^}]*\})', content)
            for raw in authors_raw:
                indices = []
                mark_matches = re.finditer(r'\\altaffilmark\{([^}]*)\}', raw)
                for mm in mark_matches:
                    parts = [p.strip().replace('$', '').replace('\\', '') for p in mm.group(1).split(',')]
                    indices.extend(parts)
                super_matches = re.finditer(r'\$[\^]*\{?([\w, \-$\star\dagger]+)\}?\$', raw)
                for mm in super_matches:
                    parts = [p.strip().replace('$', '').replace('\\', '') for p in mm.group(1).split(',')]
                    indices.extend(parts)

                name = extract_name_from_author(raw)
                if not name: continue
                affs = [label_map[i] for i in indices if i in label_map]
                if affs: 
                    author_affiliations[name] = affs
                elif label_map and not indices:
                    if len(label_map) == 1:
                        author_affiliations[name] = list(label_map.values())
    return author_affiliations


def parse_elsarticle_style(section):
    author_affiliations = {}
    label_map = {}
    addr_matches = re.finditer(r'\\address\[([^\]]+)\]', section)
    for match in addr_matches:
        label = match.group(1).strip()
        start_idx = match.end()
        content = extract_balanced_content(section[start_idx-2:], r'')
        affil = clean_latex_text(content)
        if label and affil: label_map[label] = affil
    
    author_matches = re.finditer(r'\\author\[([^\]]+)\]', section)
    for match in author_matches:
        label_str = match.group(1)
        start_idx = match.end()
        content = extract_balanced_content(section[start_idx-2:], r'')
        name = extract_name_from_author(content)
        if not name: continue
        labels = [l.strip() for l in label_str.split(',') if l.strip()]
        affs = [label_map[l] for l in labels if l in label_map]
        if affs: author_affiliations[name] = affs
    return author_affiliations


def parse_robust_mapping_style(section):
    author_affiliations = {}
    label_map = {}
    for tag in ['\\affiliation', '\\institute']:
        search_idx = 0
        while True:
            match = re.search(re.escape(tag), section[search_idx:])
            if not match: break
            content = extract_balanced_content(section[search_idx + match.start():], tag)
            if content:
                blocks = re.split(r'\\and|\\\\|\\n\s*\n', content)
                for b in blocks:
                    if not b.strip(): continue
                    label_match = re.search(r'\$[\^]*\{?([\w, \-]+)\}?\$', b) or re.search(r'\\inst\{([\w, \-]+)\}', b)
                    if label_match:
                        label = label_match.group(1).strip()
                        affil = clean_latex_text(b.replace(label_match.group(0), ''))
                        if affil: label_map[label] = affil
                    else:
                        affil = clean_latex_text(b)
                        if affil:
                            next_idx = 1
                            while str(next_idx) in label_map: next_idx += 1
                            label_map[str(next_idx)] = affil
            search_idx += match.end()

    search_idx = 0
    while True:
        match = re.search(r'\\author', section[search_idx:])
        if not match: break
        content = extract_balanced_content(section[search_idx + match.start():], '\\author')
        if content:
            authors_raw = re.split(r'\\and|(?<!\{),(?![^}]*\})', content)
            for raw in authors_raw:
                name = extract_name_from_author(raw)
                if not name: continue
                indices = []
                inst_matches = re.finditer(r'\\inst\{([\w, \-]+)\}', raw)
                for m in inst_matches: indices.extend([i.strip() for i in m.group(1).split(',')])
                super_matches = re.finditer(r'\$[\^]*\{?([\w, \-]+)\}?\$', raw)
                for m in super_matches: indices.extend([i.strip() for i in m.group(1).split(',')])
                
                if indices:
                    affs = [label_map[i] for i in indices if i in label_map]
                    if affs: author_affiliations[name] = affs
                elif label_map:
                    if len(label_map) == 1: author_affiliations[name] = list(label_map.values())
                    elif "1" in label_map: author_affiliations[name] = [label_map["1"]]
        search_idx += match.end()
    return author_affiliations


def parse_latex_section(section):
    # Try Parbox first (Phase 9 addition)
    res = parse_parbox_style(section)
    if res: return res

    res = parse_altaffil_style(section)
    if res: return res
    
    res = parse_elsarticle_style(section)
    if res: return res
    
    res = parse_robust_mapping_style(section)
    if res: return res
    return {}


def parse_filtered_file(filepath):
    paper_affiliations = {}
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except: return {}
    paper_sections = re.split(r'(?=PAPER:)', content)
    current_title = None
    for section in paper_sections:
        title_match = re.search(r'PAPER:\s*(.+?)(?:\n|$)', section)
        if title_match:
            current_title = title_match.group(1).strip()
        
        if 'AFFILIATION SECTION:' in section or (current_title and 'ERROR:' not in section):
            if current_title:
                if "rate of extreme coronal line emitters" in current_title.lower():
                    print(f"DEBUG: Processing section for '{current_title}'")
                    author_affils = parse_latex_section(section)
                    print(f"DEBUG: Resulting affils: {author_affils}")
                else:
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

def names_match(name1, name2):
    if not name1 or not name2: return False
    n1, n2 = normalize_name(name1), normalize_name(name2)
    if n1 == n2: return True
    parts1, parts2 = n1.split(), n2.split()
    if not parts1 or not parts2: return False
    if parts1[-1] != parts2[-1]: return False
    if len(parts1) == 1 or len(parts2) == 1: return True
    initials1 = set(p[0] for p in parts1[:-1] if p)
    initials2 = set(p[0] for p in parts2[:-1] if p)
    return bool(initials1 & initials2)


_common_words = {'the', 'a', 'an', 'of', 'in', 'on', 'for', 'and', 'with', 'from', 'to', 'by', 'its', 'their', 'using', 'study', 'new', 'based'}

def match_author_to_affiliations(author_name, paper_title, preprocessed):
    title_words1 = set(normalize_name(paper_title).split()) - _common_words
    if not title_words1: return None
    min_overlap = min(3, len(title_words1))
    
    debug = "rate of extreme coronal line" in paper_title.lower()
    
    for t, data in preprocessed.items():
        if len(title_words1 & data['words']) >= min_overlap:
            for la, affs in data['authors'].items():
                if names_match(author_name, la): return affs
                if debug and "callow" in author_name.lower():
                    print(f"DEBUG MATCH FAIL: CSV: '{author_name}' vs LATEX: '{la}'")
                    print(f"Norm CSV: '{normalize_name(author_name)}'")
                    print(f"Norm LATEX: '{normalize_name(la)}'")
    return None


def main():
    print("=" * 60)
    print("Affiliation Filler V9 - Parbox & Manual Formatting")
    print("=" * 60)
    
    df = pd.read_csv(CSV_PATH)
    all_paper_affiliations = {}
    for f in LATEX_FILES:
        print(f"Parsing {f}...")
        all_paper_affiliations.update(parse_filtered_file(f))
    
    preprocessed = {t: {'words': set(normalize_name(t).split()) - _common_words, 'authors': a} for t, a in all_paper_affiliations.items()}
    
    # DEBUG: Check if target paper is in preprocessed
    debug_target = "rate of extreme coronal line emitters"
    found_debug = False
    for t in preprocessed:
        if debug_target in t.lower():
            print(f"DEBUG: Found target paper in preprocessed: {t}")
            print(f"DEBUG: Authors found: {preprocessed[t]['authors'].keys()}")
            found_debug = True
            break
    if not found_debug:
        print(f"DEBUG: Target paper '{debug_target}' NOT found in preprocessed data.")

    def is_missing(x):
        try:
            affs = ast.literal_eval(str(x))
            return isinstance(affs, list) and any(a is None for a in affs)
        except: return True

    missing_indices = df[df['affiliations'].apply(is_missing)].index.tolist()
    print(f"Processing {len(missing_indices)} papers.")

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

        valid_affs = [a for a in affiliations if a]
        if None in affiliations and len(set(valid_affs)) == 1:
            shared = valid_affs[0]
            affiliations = [shared if a is None else a for a in affiliations]
            changed = True
        if changed: df.at[idx, 'affiliations'] = str(affiliations)

    df.to_csv(OUTPUT_CSV_PATH, index=False)
    
    fully_filled_count = 0
    partially_filled_count = 0
    total_null_author_slots = 0
    incomplete_indices = []
    total_papers = len(df)

    for idx, row in df.iterrows():
        try:
            affs = ast.literal_eval(str(row['affiliations']))
            null_count = sum(1 for a in affs if a is None)
            total_null_author_slots += null_count
            if null_count == 0: fully_filled_count += 1
            else:
                partially_filled_count += 1
                incomplete_indices.append(idx)
        except: incomplete_indices.append(idx)

    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write("=== MISSING AFFILIATIONS REPORT (V9) ===\n\n")
        f.write(f"Total papers: {total_papers}\n")
        f.write(f"Fully filled papers: {fully_filled_count}\n")
        f.write(f"Partially filled papers: {partially_filled_count}\n")
        f.write(f"Total remaining null author affiliations: {total_null_author_slots}\n")
        f.write(f"\nRemaining incomplete indices ({len(incomplete_indices)}):\n")
        f.write(str(incomplete_indices) + "\n")
    
    print(f"Done. Results in {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    main()
