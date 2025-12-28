r"""
Extract authors and affiliations from LaTeX files.

This script handles multiple LaTeX author/affiliation formats by using a
normalization pre-pass that converts everything to a standard format:
    \author{Name1\inst{a} and Name2\inst{b}}
    \institute{$^a$ Affiliation1\\$^b$ Affiliation2}

Supported input formats:
1. Standard format (already normalized):
   - \author{Name\inst{a} and Name\inst{b}}
   - \institute{$^a$ University1\\$^b$ University2}

2. Alternative format with numeric codes:
   - \Author{Name $^{1,2}$, Name $^{3}$ and Name $^{4}$}
   - \address{$^{1}$ University1\\$^{2}$ University2...}

The normalization process:
- Converts numeric codes ($^{1}$) to letter codes (a, b, c...)
- Converts \Author to \author and \address to \institute
- Handles nested braces properly throughout extraction
- Cleans LaTeX escapes and special characters from output

Benefits:
- Single parsing path after normalization
- Easy to extend with new formats
- Robust handling of complex LaTeX structures
"""

import json
import re
import sys


def extract_braced_content(text, command):
    """Extract content from LaTeX command with properly balanced nested braces."""
    idx = text.find(command + "{")
    if idx < 0:
        return None
    start = idx + len(command) + 1
    count = 1
    i = start
    while i < len(text) and count > 0:
        if text[i] == "\\":
            i += 2  # Skip escaped characters
            continue
        if text[i] == "{":
            count += 1
        elif text[i] == "}":
            count -= 1
        i += 1
    return text[start : i - 1] if count == 0 else None


def normalize_latex_format(text):
    r"""
    Convert various LaTeX author/affiliation formats to a standard format:
    \author{Name1\inst{a} and Name2\inst{b}}
    \institute{$^a$ Affiliation1\\$^b$ Affiliation2}
    """
    # Check if already in standard format
    if r"\inst{" in text and r"\institute{" in text:
        return text

    # Format 2: \Author{Name $^{1}$, ...} with \address{...}
    author_block = extract_braced_content(text, r"\Author")
    if not author_block:
        author_block = extract_braced_content(text, r"\author")

    if not author_block:
        return text

    # If using $^{numbers}$ style, convert to \inst{} style
    if "$^{" in author_block or "$^" in author_block:
        # Extract authors with their numeric codes
        parts = re.split(r",\s*(?=[A-Z])|\s+and\s+", author_block)

        author_entries = []
        code_mapping = {}  # Map numeric codes to letter codes
        next_letter = ord("a")

        for part in parts:
            # Extract name and numeric code
            match = re.match(r"([A-Z](?:[^\$]|\\.)*?)\s*\$\^?\{?([0-9]+)", part)
            if match:
                name = match.group(1).strip()
                num_code = match.group(2)

                # Assign letter code
                if num_code not in code_mapping:
                    code_mapping[num_code] = chr(next_letter)
                    next_letter += 1

                letter_code = code_mapping[num_code]
                author_entries.append(f"{name}\\inst{{{letter_code}}}")

        # Build normalized \author{} block
        normalized_author = "\\author{" + " and ".join(author_entries) + "}"

        # Convert \address{} to \institute{}
        addr_block = extract_braced_content(text, r"\address")
        if addr_block:
            # Split affiliations
            affil_parts = re.split(r"\\\\\s*\n", addr_block)

            institute_entries = []
            for part in affil_parts:
                part = part.strip()
                if not part:
                    continue

                # Match numeric code and affiliation
                m = re.match(r"\$\^?\{?([0-9]+)\}?\$\s+\\quad\s+(.+)", part, re.S)
                if not m:
                    m = re.match(r"\$\^?\{?([0-9]+)\}?\$\s+(.+)", part, re.S)

                if m:
                    num_code = m.group(1)
                    affil = m.group(2)

                    if num_code in code_mapping:
                        letter_code = code_mapping[num_code]
                        institute_entries.append(f"$^{letter_code}$ {affil}")

            # Build normalized \institute{} block
            normalized_institute = (
                "\\institute{" + "\\\\".join(institute_entries) + "}\n"
            )

            # Find and replace blocks manually to avoid re.sub escape issues
            author_start = text.find(r"\Author{")
            if author_start >= 0:
                # Use extract_braced_content to find the end
                temp_block = extract_braced_content(text, r"\Author")
                if temp_block:
                    author_end = author_start + len(r"\Author{") + len(temp_block) + 1
                    text = text[:author_start] + normalized_author + text[author_end:]

            addr_start = text.find(r"\address{")
            if addr_start >= 0:
                temp_block = extract_braced_content(text, r"\address")
                if temp_block:
                    addr_end = addr_start + len(r"\address{") + len(temp_block) + 1
                    # Find the double newline after \address{...}
                    while addr_end < len(text) and text[addr_end] in " \n":
                        addr_end += 1
                    text = text[:addr_start] + normalized_institute + text[addr_end:]

    return text


def clean_latex_text(text):
    """Remove LaTeX commands and escapes from text."""
    # Handle accented characters
    text = re.sub(r"\\'([a-zA-Z])", r"\1", text)  # \'a -> a
    text = re.sub(r'\\"([a-zA-Z])', r"\1", text)  # \"o -> o
    text = re.sub(r"\\`([a-zA-Z])", r"\1", text)  # \`e -> e
    text = re.sub(r"\\k\{([a-zA-Z])\}", r"\1", text)  # \k{a} -> a

    # Remove LaTeX commands but keep content
    text = re.sub(r"\\mbox\{([^}]*)\}", r"\1", text)

    # Remove special characters
    text = re.sub(r"[*\$\{\}]+", "", text)

    # Clean up whitespace
    text = " ".join(text.split())
    return text.strip()


def extract_authors_and_affiliations(text):
    """
    Extract authors and their affiliations from LaTeX text.
    Returns a dictionary mapping author names to affiliations.
    """
    # Step 1: Normalize format
    text = normalize_latex_format(text)

    # Step 2: Extract author block
    author_block = extract_braced_content(text, r"\author")
    if not author_block:
        return {}

    # Step 3: Extract authors with their codes
    # Pattern: "Name\inst{a}" captures name and letter code
    # Use non-greedy match that stops at \inst, handling LaTeX escapes
    author_pattern = r"([A-Z](?:[^\\]|\\.)*?)\s*\\inst\{([a-z])\}"
    authors = re.findall(author_pattern, author_block)

    if not authors:
        return {}

    # Step 4: Extract institute block
    inst_text = extract_braced_content(text, r"\institute")
    if not inst_text:
        return {}

    # Step 5: Parse affiliations
    # Split by \\ separator
    affil_parts = re.split(r"\s*\\\\\s*", inst_text)

    affiliations = {}
    for part in affil_parts:
        part = part.strip()
        if not part:
            continue

        # Match pattern: $^a$ followed by affiliation text
        match = re.match(r"\$\^([a-z])\$\s+(.+)", part, re.S)
        if match:
            code = match.group(1)
            affil = match.group(2)

            # Remove email commands (handle nested braces)
            affil = re.sub(r"\\email\{[^}]*(?:\{[^}]*\})*[^}]*\}", "", affil)

            # Remove various LaTeX artifacts
            affil = re.sub(r"\\quad\s*", "", affil)
            affil = re.sub(r"\{\}", "", affil)
            affil = re.sub(r"\}+$", "", affil)
            affil = re.sub(r"\\\\\s*$", "", affil)

            # Remove email addresses
            affil = re.sub(r";?\s*[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}", "", affil)

            # Clean LaTeX escapes
            affil = clean_latex_text(affil)
            affil = affil.strip(" ,;\\")

            affiliations[code] = affil

    # Step 6: Build result dictionary
    result = {}
    for name, code in authors:
        # Clean up author name
        clean_name = clean_latex_text(name)

        # Skip if name is too short (likely parsing error)
        if len(clean_name) < 3:
            continue

        affiliation = affiliations.get(code, "")
        result[clean_name] = affiliation

    return result


def main():
    """Main entry point."""
    # Get filename from command line or use default
    filename = sys.argv[1] if len(sys.argv) > 1 else "latex_affiliations_output_2.txt"

    try:
        with open(filename, "r", encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        print(json.dumps({"error": f"File not found: {filename}"}, indent=2))
        sys.exit(1)

    # Extract authors and affiliations
    result = extract_authors_and_affiliations(text)

    # Output as JSON
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()