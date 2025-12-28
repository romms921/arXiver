import pandas as pd
import os

# 1. Load titles
with open('papers_with_missing_affiliations.txt', 'r') as f:
    titles = [line.strip() for line in f if line.strip()]
data = pd.DataFrame(titles, columns=['title'])
print(f"Loaded {len(data)} titles.")

# 2. Parse Latex
paper_sections = {}
current_paper = None
affiliation_started = False

try:
    with open("latex_affiliations_output.txt", "r") as f:
        for i, line in enumerate(f):
            stripped_line = line.strip()
            if line.startswith("PAPER: "):
                current_paper = line.replace("PAPER: ", "").strip()
                paper_sections[current_paper] = []
                affiliation_started = False
            elif line.startswith("AFFILIATION SECTION:"):
                affiliation_started = True
            elif line.startswith("-" * 80):
                continue
            elif current_paper and affiliation_started:
                if stripped_line:
                    paper_sections[current_paper].append(line.rstrip('\n'))
            if i % 100000 == 0:
                print(f"Read {i} lines...")
    print(f"Successfully parsed {len(paper_sections)} papers.")
except Exception as e:
    print(f"Error parsing: {e}")

# 3. Load countries
list_countries = pd.read_csv('world_coords.csv')['country'].tolist()
print(f"Loaded {len(list_countries)} countries.")

# 4. Match
results = []
normalized_sections = {k.strip().lower(): k for k in paper_sections.keys()}
for title in data['title']:
    matching_lines = []
    title_norm = str(title).strip().lower()
    if title_norm in normalized_sections:
        original_key = normalized_sections[title_norm]
        section_lines = paper_sections[original_key]
        for l in section_lines:
            for country in list_countries:
                if country in l:
                    matching_lines.append(l.strip())
    results.append({'title': title, 'lines': matching_lines})

output_df = pd.DataFrame(results)
print(f"Matched {len(output_df[output_df['lines'].map(len) > 0])} papers.")
