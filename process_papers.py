#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
from collections import defaultdict

def clean_latex_text(text):
    """
    Removes common LaTeX commands, resolves simple accents, and cleans up a string for plain text output.
    """
    if not text:
        return ""
    # Remove comments that might be on the same line
    text = re.sub(r'%.*$', '', text, flags=re.MULTILINE)
    
    # Remove commands with arguments first
    commands_with_args = [
        'orcidlink', 'orcid', 'thanks', 'email', 'corref', 'cortext', 'fnref', 'tnoteref', 
        'inst', 'label', 'altaffiliation', 'altaffilmark', 'affil', 'titlerunning', 
        'authorrunning', 'emailAdd', 'href', 'url'
    ]
    for cmd in commands_with_args:
        text = re.sub(r'\\' + cmd + r'(?:\[[^\]]*\])?\{.*?\}', '', text, flags=re.DOTALL)

    # Handle simple replacements for accents and symbols
    replacements = {
        r"\'e": "é", r"\`e": "è", r"\^e": "ê", r'\"e': "ë",
        r"\'a": "á", r"\`a": "à", r"\^a": "â", r'\"a': "ä",
        r"\'o": "ó", r"\`o": "ò", r"\^o": "ô", r'\"o': "ö",
        r"\'u": "ú", r"\`u": "ù", r"\^u": "û", r'\"u': "ü",
        r"\'i": "í", r"\`i": "ì", r"\^i": "î", r'\"i': "ï",
        r"\c{c}": "ç", r"\c{s}": "ş", r"\c{C}": "Ç", r"\c{g}": "ğ", r"\u{g}": "ğ",
        r"\~n": "ñ", r"\~o": "õ",
        r"\&": "&", r"\_": "_",
        r"\ ": " ", r"\,": " ", r"\;": " ",
        "''": '"', "``": '"',
        r'\s*\\\s*': ' ',  # Newlines
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
        
    # Remove any remaining commands like \bf, \it, etc., and math mode markers
    text = re.sub(r'\\[a-zA-Z]+', '', text)
    text = re.sub(r'[\$\{\}]', '', text)
    # Remove left-over bracket arguments
    text = re.sub(r'\[[^\]]*\]', '', text)
    # Remove superscript numbers, e.g., $^{1}$
    text = re.sub(r'\s*\^\s*\d+\s*', '', text)

    # General cleanup
    text = ' '.join(text.split())
    
    return text.strip()

def escape_csv_field(field):
    """
    Escapes a string for robust CSV format by enclosing it in double quotes
    and escaping any internal double quotes.
    """
    if field is None:
        return '""'
    # Replace any double quotes inside the string with two double quotes
    escaped_field = field.replace('"', '""')
    # Enclose the entire string in double quotes
    return f'"{escaped_field}"'

def parse_paper_block(paper_content):
    """
    Parses a single paper's LaTeX snippet to extract title, authors, and affiliations.
    It tries to automatically detect the LaTeX style used.
    """
    # --- Extract Title ---
    title = ""
    title_match = re.search(r'\\title(?:\[[^\]]*\])?\s*\{(.*?)\}', paper_content, re.DOTALL)
    if title_match:
        title = clean_latex_text(title_match.group(1))
    else:
        title_match = re.search(r'\\title\s+(.*)', paper_content)
        if title_match:
            title = clean_latex_text(title_match.group(1).split('\n')[0])

    authors_data = []

    # --- Strategy detection and parsing ---

    # Strategy 1: aa-style (\author{...\inst{...}} and \institute{...\label{...}})
    if r'\inst' in paper_content and r'\institute' in paper_content:
        affil_map = {}
        institutes = re.findall(r'\\institute\{(.*?)\s*(?:\\label\{inst(\d+)\})?\}', paper_content, re.DOTALL)
        if not institutes:
             institutes_raw = re.findall(r'\\institute(.*?)(?=\\institute|\\date|\\abstract|\\maketitle)', paper_content, re.DOTALL)
             for i, block in enumerate(institutes_raw):
                 lines = [line.strip() for line in block.strip().split('\n') if line.strip()]
                 affil_map[str(i+1)] = ' '.join(clean_latex_text(line) for line in lines)
        else:
            for i, (inst_text, inst_label) in enumerate(institutes):
                label = inst_label if inst_label else str(i + 1)
                affil_map[label] = clean_latex_text(inst_text)
        
        author_block_match = re.search(r'\\author\{(.*?)\}', paper_content, re.DOTALL)
        if author_block_match:
            author_content = author_block_match.group(1)
            author_chunks = re.split(r'\s*\\and\s*', author_content)
            for chunk in author_chunks:
                name_match = re.search(r'^(.*?)(?:\\inst\{([^}]+)\})?', chunk.strip(), re.DOTALL)
                if name_match:
                    name = clean_latex_text(name_match.group(1))
                    inst_tags_str = name_match.group(2)
                    affiliations = []
                    if inst_tags_str:
                        inst_tags = inst_tags_str.replace(' ', '').split(',')
                        affiliations = [affil_map.get(tag, f"Unresolved Inst({tag})") for tag in inst_tags]
                    authors_data.append({'name': name, 'affiliations': affiliations})
        if authors_data:
            return title, authors_data

    # Strategy 2: revtex/aastex-style (sequential \author, \affiliation)
    pattern = re.compile(r'\\author(?:\[[^\]]*\])?\{(.*?)\}|\\affiliation(?:\[[^\]]*\])?\{(.*?)\}', re.DOTALL)
    matches = pattern.finditer(paper_content)
    
    current_author = None
    for match in matches:
        if match.group(1):
            if current_author:
                authors_data.append(current_author)
            name = clean_latex_text(match.group(1))
            current_author = {'name': name, 'affiliations': []}
        elif match.group(2) and current_author:
            affiliation = clean_latex_text(match.group(2))
            current_author['affiliations'].append(affiliation)
            
    if current_author:
        authors_data.append(current_author)

    if authors_data:
        return title, authors_data

    # Strategy 3: mnras/spie-style (single \author block with numbered affiliations below)
    author_block_match = re.search(r'\\author\[[^\]]*\]\{(.*?)\}|\\author\{(.*?)\}', paper_content, re.DOTALL)
    if author_block_match:
        content = author_block_match.group(1) or author_block_match.group(2)
        if r'\\' in content and re.search(r'\$\^\{\d+\}', content):
            parts = re.split(r'\\\\', content, maxsplit=1)
            author_lines, affil_lines = parts[0], parts[1]
            
            affil_map = {}
            affil_matches = re.findall(r'\$\^\{(\d+)\}(.*?)(?=\$\^\{\d+\}|$)', affil_lines, re.DOTALL)
            for num, affil in affil_matches:
                 affil_map[num] = clean_latex_text(affil)

            author_name_chunks = re.split(r',|\s*\\and\s*|\s*\\newauthor\s*', author_lines)
            for chunk in author_name_chunks:
                if not chunk.strip(): continue
                affil_nums = re.findall(r'\$\^\{(\d+)\}', chunk)
                name = clean_latex_text(re.sub(r'\$\^\{[^\}]*\}', '', chunk))
                if name:
                    affiliations = [affil_map.get(num, f"Unresolved Affil({num})") for num in affil_nums]
                    authors_data.append({'name': name, 'affiliations': affiliations})
            if authors_data:
                return title, authors_data

    return title, authors_data

def process_file(filename):
    """
    Reads a file, splits it into paper blocks, and processes each block into
    a robust CSV format.
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.", file=sys.stderr)
        return
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        return

    paper_blocks = re.split(r'\n-{20,}\n', content)
    
    output_blocks = []
    for i, block in enumerate(paper_blocks):
        block = block.strip()
        if not block:
            continue

        fallback_title_match = re.search(r'PAPER:\s*(.*)', block)
        fallback_title = fallback_title_match.group(1).strip() if fallback_title_match else f"Untitled Paper {i+1}"
        
        title, parsed_authors = parse_paper_block(block)
        if not title:
            title = fallback_title
        
        if not parsed_authors:
            continue
        
        paper_output = []
        for author_info in parsed_authors:
            name = author_info['name']
            if not name: continue
            
            affiliations = '; '.join(filter(None, [aff.strip() for aff in author_info['affiliations']]))
            
            # Create a properly formatted CSV line
            line_parts = [
                escape_csv_field(title),
                escape_csv_field(name),
                escape_csv_field(affiliations)
            ]
            paper_output.append(','.join(line_parts))
        
        if paper_output:
            output_blocks.append('\n'.join(paper_output))
            
    # Print the final result with blank lines between papers
    print('\n\n'.join(output_blocks))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python process_papers.py <papers.txt>", file=sys.stderr)
        sys.exit(1)
    
    input_filename = sys.argv[1]
    process_file(input_filename)