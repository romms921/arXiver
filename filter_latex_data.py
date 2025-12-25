import pandas as pd
import ast
import re

def get_fully_filled_titles(csv_path):
    df = pd.read_csv(csv_path)
    fully_filled_titles = []
    for idx, row in df.iterrows():
        try:
            affs = ast.literal_eval(str(row['affiliations']))
            # If it's a list and has no None values, it's fully filled
            if isinstance(affs, list) and all(a is not None for a in affs):
                fully_filled_titles.append(row['title'].strip())
        except:
            continue
    return set(fully_filled_titles)

def filter_file(filepath, output_path, fully_filled_titles):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split by the separator used in the output files
    # The separator is at least 70 dashes
    papers = re.split(r'-{70,}', content)
    
    filtered_papers = []
    for paper in papers:
        title_match = re.search(r'PAPER:\s*(.+?)(?:\n|$)', paper)
        if title_match:
            title = title_match.group(1).strip()
            if title not in fully_filled_titles:
                filtered_papers.append(paper)
        else:
            # If no title found (header/footer/empty), maybe keep or discard
            if paper.strip():
                filtered_papers.append(paper)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("-" * 80 + "\n".join(filtered_papers))
    
    print(f"Filtered {filepath} -> {output_path}")

def main():
    csv_path = 'test_filled_8.csv'
    titles = get_fully_filled_titles(csv_path)
    print(f"Found {len(titles)} fully filled papers.")

    filter_file('latex_affiliations_output.txt', 'latex_filtered_1.txt', titles)
    filter_file('latex_affiliations_output_2.txt', 'latex_filtered_2.txt', titles)

if __name__ == '__main__':
    main()
