
import re
import pandas as pd

def extract_balanced_content(text, start_token):
    if not text: return None
    pattern = re.escape(start_token) + r'\s*\{'
    match = re.search(pattern, text)
    if not match: return None
    start_idx = match.end()
    stack = 1
    for i in range(start_idx, len(text)):
        if text[i] == '{': stack += 1
        elif text[i] == '}':
            stack -= 1
            if stack == 0: return text[start_idx:i]
    return None

def clean_latex_text(text):
    if not text: return ""
    text = re.sub(r'\\url\{[^}]*\}', '', text) # added for test
    text = re.sub(r'\\email\{[^}]*\}', '', text)
    text = re.sub(r'\\thanks\{[^}]*\}', '', text)
    text = re.sub(r'\\[a-zA-Z]+\{((?:[^{}]|\{[^{}]*\})*)\}', r'\1', text)
    text = re.sub(r'\\[a-zA-Z]+', ' ', text)
    text = re.sub(r'[{}]', ' ', text)
    text = text.replace('\\\\', ', ')
    text = text.replace('\\', ' ')
    return re.sub(r'\s+', ' ', text).strip()

def extract_name_from_author(author_str):
    author_str = re.sub(r'\\altaffilmark\{[^}]*\}', '', author_str)
    author_str = re.sub(r'\\orcidlink\{[^}]*\}', '', author_str)
    return clean_latex_text(author_str)

section = r"""
\title{Long-lived Habitable Zones around White Dwarfs undergoing Neon-22 Distillation}
\author{Andrew Vanderburg\altaffilmark{1,$\star$}\orcidlink{0000-0001-7246-5438}, Antoine B\'edard\altaffilmark{2}\orcidlink{0000-0002-2384-1326}, Juliette C. Becker\altaffilmark{3}\orcidlink{0000-0002-7733-4522}, Simon Blouin\altaffilmark{4}\orcidlink{0000-0002-9632-1436}}
\altaffiltext{1}{Department of Physics and Kavli Institute for Astrophysics and Space Research, Massachusetts Institute of Technology, Cambridge, MA 02139, USA}
\altaffiltext{$\star$}{\url{andrewv@mit.edu}, Sloan Research Fellow}
\altaffiltext{2}{Department of Physics, University of Warwick, CV4 7AL, Coventry, UK}
\altaffiltext{3}{Department of Astronomy,  University of Wisconsin-Madison, 475 N.~Charter St., Madison, WI 53706, USA}
\altaffiltext{4}{Department of Physics and Astronomy, University of Victoria, Victoria, BC V8W 2Y2, Canada}
"""

def test_parse():
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
    
    print("Label Map:", label_map)

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
                name = extract_name_from_author(raw)
                if not name: continue
                affs = [label_map[i] for i in indices if i in label_map]
                if affs: author_affiliations[name] = affs
    
    print("Results:", author_affiliations)

if __name__ == "__main__":
    test_parse()
