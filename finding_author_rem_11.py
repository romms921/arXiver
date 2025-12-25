"""
Finding Author Affiliations - Enhanced Parser (V11 - Generalized Nested Tags)

Handles:
1. \parbox{\textwidth} blocks for authors and affiliations.
2. Manual superscript mapping (e.g. ^{1}) in parbox blocks.
3. Inherits all previous parser logic (Altaffil, Nested Braces, Elsevier, etc.).
4. NEW: \affiliation[...] support.
5. NEW: MNRAS style (\author[...] containing authors and affiliations).
6. NEW: Generalized nested tags (\aff{...}) in authors, affiliations, parbox, institute.
7. NEW: Updates latex_filtered files by removing processed papers.
"""

import pandas as pd
import ast
import re
import os
from collections import defaultdict
from tqdm import tqdm

# ========================
# CONFIG
# ========================

CSV_PATH = "test_filled_10.csv"
OUTPUT_CSV_PATH = "test_filled_11.csv"
LATEX_FILES = ["latex_filtered_1.txt", "latex_filtered_2.txt"]
REPORT_PATH = "missing_affiliations_report.txt"


# ========================
# PARSING UTILITIES
# ========================

def extract_balanced_content(text, start_token):
    """
    Extracts content within balanced curly braces following start_token.
    Supports optional [...] arguments before the opening brace, e.g., \command[opt]{content}.
    """
    if not text: return None
    # Updated pattern to allow optional square brackets [ ... ] before {
    pattern = re.escape(start_token) + r'(?:\s*\[[^\]]*\])?\s*\{'
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
    text = re.sub(r'\\aff\{[^}]*\}', '', text) # Remove label tags from text body if they remain
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
    author_str = re.sub(r'\\aff\{[^}]*\}', '', author_str) # Remove \aff{...} from name
    author_str = re.sub(r'\\orcidlink\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\footnote\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\$[\^]*\{?[\w, \-$\star\dagger]*\}?\$', '', author_str)
    name = clean_latex_text(author_str)
    return name.strip()


def parse_parbox_style(section):
    """
    Handles \parbox{\textwidth}{...authors...} followed by \parbox{\textwidth}{...affiliations...}
    Also handles \aff{...} inside parboxes.
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

    affil_keywords = ['university', 'institute', 'department', 'school', 'laboratory', 'center', 'observatory']
    
    affil_block = None
    author_block = None
    
    for content in parbox_contents:
        lower_content = content.lower()
        # Should contain affiliation keywords
        # AND check if it looks like a list (\\) or contains \aff tags
        if (any(kw in lower_content for kw in affil_keywords) and ('\\\\' in content or '\\aff' in content)) or '\\affiliation' in content:
            affil_block = content
        else:
            if '$^' in content or '\\thanks' in content or '\\aff' in content:
                author_block = content
    
    if not affil_block and len(parbox_contents) >= 2:
        affil_block = parbox_contents[-1]
        author_block = parbox_contents[0]

    if not affil_block or not author_block: return {}

    # Parse Affiliations with generalized splitter
    # Can be separated by \\, \par, or \aff{ID}
    
    # Pre-process: replace \aff{ID} with a delimiter
    # Pattern for \aff{ID}: \\aff\{([\w, \-]+)\}
    
    # Store labels found via \aff in a temp map
    # We want to split the string such that we keep the label.
    
    pass_1_blocks = []
    
    # Check if \aff tags are present
    if '\\aff{' in affil_block:
        # Split by \aff{...}
        parts = re.split(r'(\\aff\{[^}]+\})', affil_block)
        # parts will be [preamble, \aff{1}, text, \aff{2}, text...]
        current_label = None
        current_text = ""
        
        for p in parts:
            aff_match = re.match(r'\\aff\{([^}]+)\}', p)
            if aff_match:
                # Save previous
                if current_label and current_text.strip():
                    label_map[current_label] = clean_latex_text(current_text)
                
                current_label = aff_match.group(1).strip()
                current_text = ""
            else:
                current_text += p
        
        # Save last
        if current_label and current_text.strip():
            label_map[current_label] = clean_latex_text(current_text)
            
    else:
        # Fallback to standard split
        lines = re.split(r'\\\\|\\par', affil_block)
        for line in lines:
            line = line.strip()
            if not line: continue
            
            label_match = re.search(r'\$[\^]*\{?([\w, \-]+)\}?\$', line)
            if label_match:
                label = label_match.group(1).strip()
                affil_text = line.replace(label_match.group(0), '')
                affil = clean_latex_text(affil_text)
                if affil: label_map[label] = affil

    # Parse Authors
    cleaned_author_block = re.sub(r'\\and',('\n'), author_block)
    # Split by \aff{...} is not for splitting authors, but for finding indices
    
    names_raw = []
    if '\n' in cleaned_author_block:
         names_raw = cleaned_author_block.split('\n')
    else:
         names_raw = re.split(r'(?<!\{),(?![^}]*\})', cleaned_author_block)

    for raw in names_raw:
        if not raw.strip(): continue
        name = extract_name_from_author(raw)
        if not name: continue
        
        indices = []
        # Standard superscripts
        super_matches = re.finditer(r'\$[\^]*\{?([\w, \-]+)\}?\$', raw)
        for m in super_matches:
            content = m.group(1)
            if 'orcid' in content.lower(): continue
            parts = [p.strip() for p in content.split(',')]
            indices.extend(parts)
        
        # \aff{...} tags
        aff_matches = re.finditer(r'\\aff\{([^}]+)\}', raw)
        for m in aff_matches:
            parts = [p.strip() for p in m.group(1).split(',')]
            indices.extend(parts)

        if indices:
            affs = [label_map[i] for i in indices if i in label_map]
            if affs: author_affiliations[name] = affs
    
    return author_affiliations


def parse_mnras_style(section):
    """
    Handles MNRAS style (\author[...]{...}).
    Added support for \aff{} tags inside.
    """
    if 'documentclass' not in section and 'mnras' not in section and 'author[' not in section:
        return {}

    author_affiliations = {}
    label_map = {}
    
    author_block = extract_balanced_content(section, '\\author')
    if not author_block: return {}

    parts = re.split(r'\\\\', author_block)
    
    candidate_affil_lines = []
    candidate_author_parts = []
    
    for part in parts:
        part = part.strip()
        if not part: continue
        if re.match(r'^\s*[\$\{]\^', part) or re.match(r'^\s*\^', part) or '\\aff{' in part:
            candidate_affil_lines.append(part)
        else:
            candidate_author_parts.append(part)
            
    if not candidate_affil_lines and len(parts) > 1:
        last = parts[-1]
        if 'University' in last or 'Institute' in last or 'Department' in last:
             candidate_affil_lines.append(last)
             candidate_author_parts = parts[:-1]
    
    if not candidate_affil_lines and not candidate_author_parts:
        candidate_author_parts = [author_block] 

    # Parse Affiliations
    all_affil_text = "\n".join(candidate_affil_lines)
    all_affil_text = re.sub(r'[\r\n]+', ' ', all_affil_text)
    
    # Support \aff{ID} in MNRAS too (unlikely but safe)
    if '\\aff{' in all_affil_text:
         parts = re.split(r'(\\aff\{[^}]+\})', all_affil_text)
         current_label = None
         current_text = ""
         for p in parts:
            aff_match = re.match(r'\\aff\{([^}]+)\}', p)
            if aff_match:
                if current_label and current_text.strip():
                    label_map[current_label] = clean_latex_text(current_text)
                current_label = aff_match.group(1).strip()
                current_text = ""
            else:
                current_text += p
         if current_label and current_text.strip():
            label_map[current_label] = clean_latex_text(current_text)
    else:
        # Regex for $^{1}$
        matches = list(re.finditer(r'(\$[\^]*\{?[\w, \-]+\}?\$|\^\{?[\w, \-]+\}?)', all_affil_text))
        for i, match in enumerate(matches):
            label_raw = match.group(1)
            label_match = re.search(r'[\^]\{?([\w, \-]+)\}?', label_raw)
            if not label_match: continue
            label = label_match.group(1).strip()
            
            start = match.end()
            end = matches[i+1].start() if i+1 < len(matches) else len(all_affil_text)
            affil_text = all_affil_text[start:end]
            
            clean_aff = clean_latex_text(affil_text)
            if clean_aff: label_map[label] = clean_aff

    # Parse Authors
    all_author_text = " ".join(candidate_author_parts)
    
    author_groups = []
    current_group = ""
    brace_level = 0
    for char in all_author_text:
        if char == '{':
            if brace_level > 0: current_group += char
            brace_level += 1
        elif char == '}':
            brace_level -= 1
            if brace_level > 0: current_group += char
            elif brace_level == 0:
                if current_group.strip(): author_groups.append(current_group.strip())
                current_group = ""
        else:
            if brace_level > 0: current_group += char
            
    if not author_groups:
        author_groups = re.split(r',', all_author_text)

    for group in author_groups:
        raw = group.strip()
        if not raw: continue
        name = extract_name_from_author(raw)
        if not name: continue
        
        indices = []
        super_matches = re.finditer(r'(\$[\^]*\{?([\w, \-]+)\}?\$|\^\{?([\w, \-]+)\}?)', raw)
        for m in super_matches:
            content = m.group(2) if m.group(2) else m.group(3)
            if not content: continue
            if 'orcid' in content.lower() or 'thanks' in content.lower(): continue
            parts = [p.strip() for p in content.split(',')]
            indices.extend(parts)
            
        aff_matches = re.finditer(r'\\aff\{([^}]+)\}', raw)
        for m in aff_matches:
            parts = [p.strip() for p in m.group(1).split(',')]
            indices.extend(parts)
            
        if indices:
            affs = [label_map[i] for i in indices if i in label_map]
            if affs: author_affiliations[name] = affs
            
    return author_affiliations


def parse_altaffil_style(section):
    # (Same as V10 but added safety)
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
                
                # Check for \aff tags too
                aff_matches = re.finditer(r'\\aff\{([^}]+)\}', raw)
                for m in aff_matches:
                    parts = [p.strip() for p in m.group(1).split(',')]
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
    # (Same as V10)
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
    """
    Handles \affiliation, \institute, and now \aff{ID} nesting.
    """
    author_affiliations = {}
    label_map = {}
    
    # We look for \affiliation, \institute, and maybe others if needed
    for tag in ['\\affiliation', '\\institute']:
        search_idx = 0
        while True:
            # find next tag
            match = re.search(re.escape(tag) + r'(?:\[[^\]]*\])?', section[search_idx:])
            if not match: break
            
            content = extract_balanced_content(section[search_idx:], tag)
            
            if content:
                # Determine tag label if any (e.g. \affiliation[1]{...})
                full_match = re.search(re.escape(tag) + r'(?:\s*\[([^\]]*)\])?\s*\{', section[search_idx:])
                tag_label = full_match.group(1).strip() if full_match and full_match.group(1) else None
                
                # Check for \aff{...} structure inside
                if '\\aff{' in content:
                     parts = re.split(r'(\\aff\{[^}]+\})', content)
                     current_label = None
                     current_text = ""
                     
                     for p in parts:
                        aff_match = re.match(r'\\aff\{([^}]+)\}', p)
                        if aff_match:
                            if current_label and current_text.strip():
                                label_map[current_label] = clean_latex_text(current_text)
                            current_label = aff_match.group(1).strip()
                            current_text = ""
                        else:
                            current_text += p
                     if current_label and current_text.strip():
                        label_map[current_label] = clean_latex_text(current_text)
                else:
                    # Regular split
                    blocks = re.split(r'\\and|\\\\|\\n\s*\n', content)
                    for b in blocks:
                        if not b.strip(): continue
                        label_match = re.search(r'\$[\^]*\{?([\w, \-]+)\}?\$', b) or re.search(r'\\inst\{([\w, \-]+)\}', b)
                        if label_match:
                            label = label_match.group(1).strip()
                            affil = clean_latex_text(b.replace(label_match.group(0), ''))
                            if affil: label_map[label] = affil
                        elif tag_label:
                            affil = clean_latex_text(b)
                            if affil: label_map[tag_label] = affil
                        else:
                            affil = clean_latex_text(b)
                            if affil:
                                next_idx = 1
                                while str(next_idx) in label_map: next_idx += 1
                                label_map[str(next_idx)] = affil
                            
                search_idx += full_match.end() + len(content) if full_match else match.end() + len(content)
            else:
                search_idx += match.end()

    # Parsing Authors associated
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
                
                # \aff{...} support
                aff_matches = re.finditer(r'\\aff\{([^}]+)\}', raw)
                for m in aff_matches: indices.extend([i.strip() for i in m.group(1).split(',')])
                
                if indices:
                    affs = [label_map[i] for i in indices if i in label_map]
                    if affs: author_affiliations[name] = affs
                elif label_map:
                    if len(label_map) == 1: author_affiliations[name] = list(label_map.values())
                    elif "1" in label_map: author_affiliations[name] = [label_map["1"]]
        search_idx += match.end()
    return author_affiliations


def parse_latex_section(section):
    # Try Parbox (for multi-col formatting)
    res = parse_parbox_style(section)
    if res: return res

    # Try MNRAS style (complex author block)
    res = parse_mnras_style(section)
    if res: return res

    # Try Altaffil (AAS style)
    res = parse_altaffil_style(section)
    if res: return res
    
    # Try Elsevier
    res = parse_elsarticle_style(section)
    if res: return res
    
    # Robust mapping (Default) - now handles \aff
    res = parse_robust_mapping_style(section)
    if res: return res
    return {}


def parse_filtered_file(filepath):
    paper_affiliations = {}
    if not os.path.exists(filepath): return {}
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
    
    for t, data in preprocessed.items():
        if len(title_words1 & data['words']) >= min_overlap:
            for la, affs in data['authors'].items():
                if names_match(author_name, la): return affs
    return None

def update_latex_files(processed_titles, files):
    """
    Rewrites latex files, removing sections corresponding to papers that are marked as done (fully filled).
    This assumes processed_titles contains titles of papers that are NOW fully complete.
    """
    # NOTE: The user asked to remove "papers that are done".
    # We should define "done" as fully filled in the CSV.
    # However, this function is called at the end. We need the list of fully filled papers from the DF.
    
    print("\nUpdating Latex Files (Removing processed papers)...")
    
    processed_titles_normalized = {normalize_name(t) for t in processed_titles}
    
    for filepath in files:
        if not os.path.exists(filepath): continue
        
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
        new_content_parts = []
        paper_sections = re.split(r'(?=PAPER:)', content)
        
        removed_count = 0
        
        for section in paper_sections:
            if not section.strip(): 
                new_content_parts.append(section)
                continue
                
            title_match = re.search(r'PAPER:\s*(.+?)(?:\n|$)', section)
            if title_match:
                title = title_match.group(1).strip()
                if normalize_name(title) in processed_titles_normalized:
                    removed_count += 1
                    continue # Skip this section
            
            new_content_parts.append(section)
            
        if removed_count > 0:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("".join(new_content_parts))
            print(f"  Updated {filepath}: Removed {removed_count} papers.")
        else:
            print(f"  {filepath}: No changes.")


def main():
    print("=" * 60)
    print("Affiliation Filler V11 - Nested Tags & Cleanup")
    print("=" * 60)
    
    df = pd.read_csv(CSV_PATH)
    all_paper_affiliations = {}
    for f in LATEX_FILES:
        print(f"Parsing {f}...")
        all_paper_affiliations.update(parse_filtered_file(f))
    
    preprocessed = {t: {'words': set(normalize_name(t).split()) - _common_words, 'authors': a} for t, a in all_paper_affiliations.items()}
    
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
    
    # Generate Report & Identify Done Papers
    fully_filled_count = 0
    partially_filled_count = 0
    total_null_author_slots = 0
    incomplete_indices = []
    total_papers = len(df)
    
    done_titles = []

    for idx, row in df.iterrows():
        try:
            affs = ast.literal_eval(str(row['affiliations']))
            null_count = sum(1 for a in affs if a is None)
            total_null_author_slots += null_count
            if null_count == 0: 
                fully_filled_count += 1
                done_titles.append(row['title'])
            else:
                partially_filled_count += 1
                incomplete_indices.append(idx)
        except: 
            incomplete_indices.append(idx)

    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write("=== MISSING AFFILIATIONS REPORT (V11) ===\n\n")
        f.write(f"Total papers: {total_papers}\n")
        f.write(f"Fully filled papers: {fully_filled_count}\n")
        f.write(f"Partially filled papers: {partially_filled_count}\n")
        f.write(f"Total remaining null author affiliations: {total_null_author_slots}\n")
        f.write(f"\nRemaining incomplete indices ({len(incomplete_indices)}):\n")
        f.write(str(incomplete_indices) + "\n")
    
    print(f"Done processing. Results in {OUTPUT_CSV_PATH}")
    
    # Update Latex Files
    update_latex_files(done_titles, LATEX_FILES)

if __name__ == "__main__":
    main()
