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

        tar = tarfile.open(fileobj=io.BytesIO(r.content), mode="r:gz")
        tex_files = []

        for member in tar.getmembers():
            if member.name.endswith(".tex"):
                tex_files.append(
                    tar.extractfile(member).read().decode(errors="ignore")
                )

        return tex_files
    except Exception:
        return None


def extract_author_block(tex_files):
    for tex in tex_files:
        if "\\begin{abstract}" in tex:
            pre = tex.split("\\begin{abstract}")[0]
            if "\\author" in pre:
                return pre
    return None


# ========================
# PARSING
# ========================

def clean_tex(s):
    s = re.sub(r"\\thanks\{.*?\}", "", s)
    s = re.sub(r"\\altaffiliation\{(.*?)\}", r"\1", s)
    s = re.sub(r"\$.*?\$", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def parse_authors(block):
    authors = []

    for m in re.finditer(r"\\author(?:\[[^\]]*\])?\{([^}]*)\}", block):
        chunk = clean_tex(m.group(1))
        parts = re.split(r",| and ", chunk)
        authors.extend([p.strip() for p in parts if p.strip()])

    return authors


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



def build_affiliation_list(authors, affils):
    if not authors:
        return [], 0.0

    if not affils:
        return [""] * len(authors), 0.2

    # Astro papers usually list all affiliations globally
    if len(affils) >= 1:
        return [affils[0]] * len(authors), 0.85



# ========================
# MAIN LOGIC
# ========================

def process_row(row):
    arxiv_id = extract_arxiv_id(row.get("pdf_link", ""))
    if not arxiv_id:
        return None, None, 0.0

    tex_files = download_tex_sources(arxiv_id)
    time.sleep(SLEEP_SECONDS)

    if not tex_files:
        return None, None, 0.0

    block = extract_author_block(tex_files)
    if not block:
        return None, None, 0.0

    authors = parse_authors(block)
    affils = parse_affiliations(block)

    final_affils, confidence = build_affiliation_list(authors, affils)
    print("FOUND AFFILS:", affils[:2])
    return authors, final_affils, confidence


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
            authors, affils, conf = process_row(df.loc[i])

            # Success means: extracted affiliations exist AND are non-empty
            if affils and any(a.strip() for a in affils):
                df.at[i, "affiliations_auto"] = str(affils)
                df.at[i, "confidence"] = conf
                df.at[i, "needs_review"] = conf < 0.8
                success_count += 1
            else:
                fail_count += 1

            df.at[i, "processed"] = True

        print(f"Success: {success_count} | Fail: {fail_count}")

    except KeyboardInterrupt:
        print("\n⏹ Interrupted by user")

    finally:
        df.to_csv(SAVE_PATH, index=False)
        print("✔ Progress saved to disk")


if __name__ == "__main__":
    main()

print("\n========== SUMMARY ==========")
print(f"Successful extractions: {success_count}")
print(f"Failed extractions:     {fail_count}")
print(f"Total attempted:        {success_count + fail_count}")
