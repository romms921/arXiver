"""
Finding Author Affiliations - Enhanced Parser (V9 - Parbox & Manual Formatting)

Handles:
1. \parbox{\textwidth} blocks for authors and affiliations.
2. Manual superscript mapping (e.g. ^{1}) in parbox blocks.
3. Inherits all previous parser logic (Altaffil, Nested Braces, Elsevier, etc.).
4. NEW: \affiliation[...] support.
5. NEW: MNRAS style (\author[...] containing authors and affiliations).
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
OUTPUT_CSV_PATH = "test_filled10.csv"
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
    
    # Clean author block of some noise first?
    cleaned_author_block = re.sub(r'\\and', '\n', author_block)
    
    # Split by common delimiters
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
            content = m.group(1)
            if 'orcid' in content.lower(): continue
            parts = [p.strip() for p in content.split(',')]
            indices.extend(parts)
        
        if indices:
            affs = [label_map[i] for i in indices if i in label_map]
            if affs: author_affiliations[name] = affs
    
    return author_affiliations


def parse_mnras_style(section):
    """
    Handles MNRAS style where authors and affiliations are often inside a single \author definition:
    \author[Short]{
      {Auth1$^{1}$}, {Auth2$^{2}$}
      \\
      $^{1}$ Affil1
      $^{2}$ Affil2
    }
    """
    if 'documentclass' not in section and 'mnras' not in section and 'author[' not in section:
        return {}

    author_affiliations = {}
    label_map = {}
    
    # Extract the main author block
    # Note: extract_balanced_content now handles [options]
    author_block = extract_balanced_content(section, '\\author')
    if not author_block: return {}

    # It usually contains authors, then \\, then affiliations
    # But sometimes structure is complex.
    
    # Strategy: Split by \\. The part with affiliations usually looks like $^{1}$ Text
    
    parts = re.split(r'\\\\', author_block)
    
    # Assume later parts are affiliations if they start with superscripts
    # But authors can also be on multiple lines.
    
    candidate_affil_lines = []
    candidate_author_parts = []
    
    for part in parts:
        part = part.strip()
        if not part: continue
        # Check if it looks like an affiliation line: starts with $^...$ or ^{...}
        # MNRAS often uses $^{1}$ or $^{1,2}$
        if re.match(r'^\s*[\$\{]\^', part) or re.match(r'^\s*\^', part):
            candidate_affil_lines.append(part)
        else:
            candidate_author_parts.append(part)
            
    # If no explicit split, just process everything
    if not candidate_affil_lines and len(parts) > 1:
        # Check if last part is affiliation-heavy
        last = parts[-1]
        if 'University' in last or 'Institute' in last or 'Department' in last:
             candidate_affil_lines.append(last)
             candidate_author_parts = parts[:-1]
    
    if not candidate_affil_lines and not candidate_author_parts:
        # Maybe all in one block?
        candidate_author_parts = [author_block] # Fallback

    # Parse Affiliations from candidate lines
    # Split lines further if necessary (e.g. separated by newlines within the part)
    all_affil_text = "\n".join(candidate_affil_lines)
    # Sometimes separated by newlines or just tags
    
    # Regex to find label and text: $^{1}$ Text
    # Iterate through indices
    
    # Pattern: match $^...$ or ^... then text until next $^...$ or end
    # We can use a split approach
    
    # Normalize start
    all_affil_text = re.sub(r'[\r\n]+', ' ', all_affil_text)
    
    # Find all start indices
    matches = list(re.finditer(r'(\$[\^]*\{?[\w, \-]+\}?\$|\^\{?[\w, \-]+\}?)', all_affil_text))
    
    for i, match in enumerate(matches):
        label_raw = match.group(1)
        # Extract pure label
        label_match = re.search(r'[\^]\{?([\w, \-]+)\}?', label_raw)
        if not label_match: continue
        label = label_match.group(1).strip()
        
        start = match.end()
        end = matches[i+1].start() if i+1 < len(matches) else len(all_affil_text)
        affil_text = all_affil_text[start:end]
        
        clean_aff = clean_latex_text(affil_text)
        if clean_aff: label_map[label] = clean_aff

    # Parse Authors
    # Authors are usually in {Name...} or just comma separated
    all_author_text = " ".join(candidate_author_parts)
    # MNRAS authors often grouped: {Name \orcid...}
    
    # Try finding groups in braces
    author_groups = []
    
    # Simple brace extractor for top level
    # inner braces for orcid/thanks need to be ignored
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
        # Fallback to comma split if no braces
        author_groups = re.split(r',', all_author_text)

    for group in author_groups:
        raw = group.strip()
        if not raw: continue
        name = extract_name_from_author(raw)
        if not name: continue
        
        indices = []
        # Find superscripts
        # e.g. Name$^{1,2}$\orcid...
        # or Name^{1}
        
        super_matches = re.finditer(r'(\$[\^]*\{?([\w, \-]+)\}?\$|\^\{?([\w, \-]+)\}?)', raw)
        for m in super_matches:
            # group 2 or 3 has the content
            content = m.group(2) if m.group(2) else m.group(3)
            if not content: continue
            
            if 'orcid' in content.lower() or 'thanks' in content.lower(): continue
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
            # Updated: look for optional [...]
            match = re.search(re.escape(tag) + r'(?:\[[^\]]*\])?', section[search_idx:])
            if not match: break
            
            # extract_balanced_content will handle the Brace part, but we need to start passing from the tag start
            # actually extract_balanced_content takes the text and start_token to find it again?
            # No, extract_balanced_content finds start_token in text.
            # If we pass section[search_idx:], it finds the first one.
            # So straightforward call is fine, but we need to advance search_idx correctly.
            
            content = extract_balanced_content(section[search_idx:], tag)
            
            if content:
                # Need to find the end of this content to advance search_idx
                # We can just advance by finding the content again? 
                # Or better, since extract_balanced_content doesn't return index, 
                # we have to assume the next one is after.
                
                # Check for label in the tag arguments?
                # \affiliation[label]{text}
                # The extract_balanced_content regex consumes [label].
                
                # We need to extract the label if present.
                # Updated regex to match extract_balanced_content (allow space before [)
                full_match = re.search(re.escape(tag) + r'(?:\s*\[([^\]]*)\])?\s*\{', section[search_idx:])
                tag_label = full_match.group(1).strip() if full_match and full_match.group(1) else None
                
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
                            
                # Advance search_idx
                if full_match:
                    search_idx += full_match.end() + len(content)
                else:
                     # Fallback if full_match somehow failed but extract_balanced_content didn't
                     # This shouldn't happen with identical regex, but safe fallback:
                     search_idx += match.end() + len(content)
            else:
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
    
    # Robust mapping (Default)
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


def main():
    print("=" * 60)
    print("Affiliation Filler V10 - Edge Cases Parsing")
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
        f.write("=== MISSING AFFILIATIONS REPORT (V10) ===\n\n")
        f.write(f"Total papers: {total_papers}\n")
        f.write(f"Fully filled papers: {fully_filled_count}\n")
        f.write(f"Partially filled papers: {partially_filled_count}\n")
        f.write(f"Total remaining null author affiliations: {total_null_author_slots}\n")
        f.write(f"\nRemaining incomplete indices ({len(incomplete_indices)}):\n")
        f.write(str(incomplete_indices) + "\n")
    
    print(f"Done. Results in {OUTPUT_CSV_PATH}")

if __name__ == "__main__":
    main()
