import pandas as pd
import requests
import tarfile
import io
import re
import time
import os
import ast


# ========================
# CONFIG
# ========================

CSV_PATH = r"test.csv"     # input + working file
SAVE_PATH = r"test_with_affils.csv"     # output file
BATCH_SIZE = 5                         # save every N rows
SLEEP_SECONDS = 1.0

# ========================
# UTILITIES
# ========================

def extract_arxiv_id(pdf_link):
    if not isinstance(pdf_link, str):
        return None
    m = re.search(r'arxiv\.org/(pdf|abs)/([0-9.]+)', pdf_link)
    return m.group(2) if m else None

def affiliations_need_fix(val):
    """
    Returns True if affiliations is a list and contains None
    """
    if pd.isna(val):
        return True

    if isinstance(val, str):
        try:
            val = ast.literal_eval(val)
        except Exception:
            return True

    if isinstance(val, list):
        return any(v is None or str(v).strip().lower() == "none" for v in val)

    return True


def download_tex_sources(arxiv_id):
    try:
        url = f"https://arxiv.org/e-print/{arxiv_id}"
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return None

        fileobj = io.BytesIO(r.content)

        # Try gzip, fallback to plain tar
        try:
            tar = tarfile.open(fileobj=fileobj, mode="r:gz")
        except tarfile.ReadError:
            tar = tarfile.open(fileobj=fileobj, mode="r:")

        tex_files = {}
        for member in tar.getmembers():
            if member.name.endswith(".tex"):
                tex_files[member.name] = (
                    tar.extractfile(member)
                    .read()
                    .decode(errors="ignore")
                )

        return tex_files  # dict now
    except Exception as e:
        return None


def extract_author_block(tex_files):
    for name, tex in tex_files.items():
        # Strip comments
        tex_nc = re.sub(r"%.*", "", tex)

        # Look for document start
        if "\\begin{document}" in tex_nc:
            body = tex_nc.split("\\begin{document}", 1)[1]
        else:
            body = tex_nc

        # Stop at abstract or maketitle
        for stop in ["\\begin{abstract}", "\\maketitle"]:
            if stop in body:
                body = body.split(stop, 1)[0]

        if "\\author" in body or "\\affiliation" in body:
            return body

    return None


# ========================
# PARSING
# ========================

def clean_tex(s):
    if not s:
        return ""

    # Remove comments
    s = re.sub(r"%.*", "", s)

    # Remove math
    s = re.sub(r"\$.*?\$", "", s)

    # Replace common formatting macros
    s = re.sub(r"\\(textbf|emph|textit|underline)\{([^}]*)\}", r"\2", s)

    # Remove thanks, footnotes
    s = re.sub(r"\\thanks\{.*?\}", "", s)

    # Line breaks & spacing
    s = s.replace("~", " ")
    s = s.replace("\\\\", " ")

    # Remove remaining macros
    s = re.sub(r"\\[a-zA-Z]+\*?(?:\[[^\]]*\])?", "", s)

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s)

    return s.strip()


def parse_authors(block):
    authors = []

    for m in re.finditer(
        r"\\author(?:\[[^\]]*\])?\{([^}]*)\}",
        block,
    ):
        chunk = clean_tex(m.group(1))

        # Remove numeric markers
        chunk = re.sub(r"\$?\^\{?\d+\}?\$?", "", chunk)

        parts = re.split(r",| and ", chunk)
        authors.extend(p.strip() for p in parts if p.strip())

    return authors


def parse_numbered_affiliations(block):
    """
    Returns dict: index -> affiliation string
    """
    affil_map = {}

    patterns = [
        r"\\affil\s*\[(\d+)\]\s*\{([^}]*)\}",
        r"\\affiliation\s*\[(\d+)\]\s*\{([^}]*)\}",
        r"\\altaffiltext\s*\{(\d+)\}\s*\{([^}]*)\}",
    ]

    for pat in patterns:
        for m in re.finditer(pat, block):
            idx = m.group(1)
            affil_map[idx] = clean_tex(m.group(2))

    return affil_map

def parse_block_affiliation(block):
    affil_map = {}

    m = re.search(r"\\affiliation\s*\{", block)
    if not m:
        return affil_map

    content = extract_brace_block(block, m.end() - 1)

    # Split on LaTeX line breaks
    lines = re.split(r"\\\\|\n", content)

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Match $^1$ Institution OR ^1 Institution
        m2 = re.match(r"\$?\^\{?(\d+)\}?\$?\s*(.*)", line)
        if not m2:
            continue

        idx = m2.group(1)
        inst = clean_tex(m2.group(2))

        if inst:
            affil_map[idx] = inst

    return affil_map


def parse_authors_with_indices(block):
    authors = []
    indices = []

    for m in re.finditer(r"\\author\{([^}]*)\}", block):
        raw = m.group(1)

        raw = strip_orcid_and_thanks(raw)

        # Extract ALL numeric superscripts safely
        # Matches ^{1,2,*} or ^1 or $^{1,2}
        sup_matches = re.findall(r"\^\{?([0-9,\s]+)\}?", raw)
        idxs = []
        for sm in sup_matches:
            idxs.extend(re.findall(r"\d+", sm))

        # Remove superscripts aggressively
        raw = re.sub(r"\$?\^\{?[^}]*\}?\$?", "", raw)

        name = clean_tex(raw)

        authors.append(name)
        indices.append(sorted(set(idxs)))

    return authors, indices

def extract_brace_block(tex, start):
    """
    Extracts {...} content starting at index `start`,
    respecting nested braces.
    """
    depth = 0
    out = []
    for i in range(start, len(tex)):
        if tex[i] == "{":
            depth += 1
            if depth == 1:
                continue
        elif tex[i] == "}":
            depth -= 1
            if depth == 0:
                break
        if depth >= 1:
            out.append(tex[i])
    return "".join(out)


def parse_affiliations(block):
    affils = []

    # Standard AASTeX
    for m in re.finditer(r"\\affiliation\{([^}]*)\}", block):
        affils.append(clean_tex(m.group(1)))

    # Inline affiliation
    for m in re.finditer(r"\\altaffiliation\{([^}]*)\}", block):
        affils.append(clean_tex(m.group(1)))

    # Numbered affiliations
    for m in re.finditer(r"\\altaffiltext\{[^}]*\}\{([^}]*)\}", block):
        affils.append(clean_tex(m.group(1)))

    # Deduplicate while preserving order
    seen = set()
    affils = [a for a in affils if not (a in seen or seen.add(a))]

    return affils



def build_affiliation_list(authors, author_indices, affil_map, fallback_affils):
    """
    Returns:
      affiliations_per_author: list[list[str]]
      confidence: float
    """
    if not authors:
        return [], 0.0

    result = []

    # Best case: explicit mapping
    if affil_map and any(author_indices):
        for idxs in author_indices:
            affs = [affil_map[i] for i in idxs if i in affil_map]
            result.append(affs)

        confidence = 0.95
        return result, confidence

    # Fallback: global affiliations apply to all
    if fallback_affils:
        for _ in authors:
            result.append(fallback_affils)
        return result, 0.75

    # Worst case
    return [[] for _ in authors], 0.2

def serialize_author_affiliations(authors, affils_per_author):
    """
    Returns a list of dicts:
    [
      {"author": str, "affiliations": [str, ...]},
      ...
    ]
    """
    if not authors or not affils_per_author:
        return []

    serialized = []

    for author, affils in zip(authors, affils_per_author):
        # Ensure list
        if not isinstance(affils, list):
            affils = []

        # Remove empty / None affiliations
        affils = [a for a in affils if a and a.strip()]

        serialized.append(
            {
                "author": author,
                "affiliations": affils,
            }
        )

    return serialized


def strip_orcid_and_thanks(s):
    s = re.sub(r"\\orcidlink\{[^}]*\}", "", s)
    s = re.sub(r"\\thanks\{[^}]*\}", "", s)
    return s


def parse_authors_and_affiliations(block):
    """
    Returns:
        authors: list of author names
        affils_per_author: list of lists of affiliations
    """
    authors = []
    affils_per_author = []

    # Pre-clean ORCID and \thanks
    block = strip_orcid_and_thanks(block)

    # Split block into author-affiliation chunks
    # Regex matches \author{...} followed by zero or more \affiliation{...}
    pattern = re.compile(
        r"\\author(?:\[[^\]]*\])?\{([^}]*)\}"  # author name
        r"((?:\s*\\affiliation\{[^}]*\})*)",  # zero or more affiliations
        re.S
    )

    for m in pattern.finditer(block):
        raw_author = clean_tex(m.group(1))
        authors.append(raw_author)

        raw_affils_block = m.group(2)
        affils = re.findall(r"\\affiliation\{([^}]*)\}", raw_affils_block, re.S)
        affils = [clean_tex(a) for a in affils if a.strip()]
        affils_per_author.append(affils)

    return authors, affils_per_author


# ========================
# MAIN LOGIC
# ========================

def process_row(row):
    arxiv_id = extract_arxiv_id(row.get("pdf_link", ""))
    if not arxiv_id:
        return [], 0.0

    tex_files = download_tex_sources(arxiv_id)
    time.sleep(SLEEP_SECONDS)

    if not tex_files:
        return [], 0.0

    block = extract_author_block(tex_files)
    if not block:
        return [], 0.0

    # --- Parse authors + indices ---
    authors, affils_per_author = parse_authors_and_affiliations(block)

    # fallback only if all authors have empty affiliations
    if not any(affils_per_author):
        # numbered/block style fallback
        authors_tmp, author_indices = parse_authors_with_indices(block)
        affil_map = parse_block_affiliation(block)
        if not affil_map:
            affil_map = parse_numbered_affiliations(block)
        fallback_affils = parse_affiliations(block)
        affils_per_author, confidence = build_affiliation_list(
            authors_tmp, author_indices, affil_map, fallback_affils
        )
    else:
        confidence = 0.95


    # --- Defensive check ---
    if not isinstance(affils_per_author, list) or any(
        not isinstance(a, list) for a in affils_per_author
    ):
        raise ValueError(
            "affils_per_author must be a list of lists (one list per author)"
        )

    # --- Serialize ---
    serialized = serialize_author_affiliations(
        authors,
        affils_per_author,
    )

    # 🔎 DEBUG PRINT
    print("Extracted affiliations:")
    for entry in serialized:
        print(f"  - {entry['author']}: {entry['affiliations']}")

    print(f"Confidence score: {confidence:.2f}")

    return serialized, confidence



def main():
    df = pd.read_csv(CSV_PATH)

    success_count = 0
    fail_count = 0
    
    # Initialize columns if missing
    for col, default in [
        ("arxiv_id", ""),
        ("affiliations_auto", ""),
        ("confidence", 0.0),
        ("needs_review", True),
        ("processed", False),
    ]:
        if col not in df.columns:
            df[col] = default

    to_process = df[df["affiliations"].apply(affiliations_need_fix)].index.tolist()
    print(f"Papers needing affiliation fix: {len(to_process)}")

    processed_since_save = 0

    try:
        for i in to_process:
            print(f"Processing row {i}")
            serialized, conf = process_row(df.loc[i])

            if serialized and any(d["affiliations"] for d in serialized):
                print("Extracted affiliations:")
                for entry in serialized:
                    print(f"  - {entry['author']}: {entry['affiliations']}")

                df.at[i, "affiliations_auto"] = str(serialized)
                df.at[i, "confidence"] = conf
                df.at[i, "needs_review"] = conf < 0.8
                success_count += 1
            else:
                print("❌ No affiliations found")
                fail_count += 1


            df.at[i, "processed"] = True

            print(f"Success: {success_count} | Fail: {fail_count}")
            print(f"Confidence score: {conf:.2f}")

    except KeyboardInterrupt:
        print("\n⏹ Interrupted by user")

    finally:
        df.to_csv(SAVE_PATH, index=False)
        print("✔ Progress saved to disk")
    
    print("\n========== SUMMARY ==========")
    print(f"Successful extractions: {success_count}")
    print(f"Failed extractions:     {fail_count}")
    print(f"Total attempted:        {success_count + fail_count}")


if __name__ == "__main__":
    main()
