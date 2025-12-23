import pandas as pd
import re

def extract_id(url):
    if pd.isna(url): return ""
    match = re.search(r'(\d{4}\.\d{4,5})', str(url))
    return match.group(1) if match else ""

print("Loading original and updated datasets...")
df_old = pd.read_csv("arxiv_papers.csv")
df_new = pd.read_csv("arxiv_papers_copy.csv")

# Create ID maps
old_map = {extract_id(row['pdf_link']): str(row['date'])[:7] for _, row in df_old.iterrows() if extract_id(row['pdf_link'])}
new_map = {extract_id(row['pdf_link']): str(row['date'])[:7] for _, row in df_new.iterrows() if extract_id(row['pdf_link'])}

migration = []

print("Finding shifts...")
for paper_id, new_month in new_map.items():
    if paper_id in old_map:
        old_month = old_map[paper_id]
        if old_month != new_month:
            migration.append({'id': paper_id, 'from': old_month, 'to': new_month})

if migration:
    df_mig = pd.DataFrame(migration)
    summary = df_mig.groupby(['from', 'to']).size().reset_index(name='count')
    print("\nDETAILED MIGRATION REPORT (Shifts between months)")
    print("="*50)
    print(summary.to_string(index=False))
else:
    print("\nNo shifts detected between arxiv_papers.csv and arxiv_papers_copy.csv.")
