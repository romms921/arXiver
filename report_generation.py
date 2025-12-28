
import pandas as pd
import ast

def generate_missing_affiliation_report():
    df = pd.read_csv("arxiver_test_filled_22.csv")
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

    with open("/kaggle/working/missing_affiliations_reports.txt", 'w', encoding='utf-8') as f:
        f.write("=== MISSING AFFILIATIONS REPORT (V12) ===\n\n")
        f.write(f"Total papers: {total_papers}\n")
        f.write(f"Fully filled papers: {fully_filled_count}\n")
        f.write(f"Partially filled papers: {partially_filled_count}\n")
        f.write(f"Total remaining null author affiliations: {total_null_author_slots}\n")
        f.write(f"\nRemaining incomplete indices ({len(incomplete_indices)}):\n")
        f.write(str(incomplete_indices) + "\n")
