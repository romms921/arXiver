import pandas as pd
import re

# Load data
data = pd.read_csv("arxiv_papers_copy.csv")

def extract_id(url):
    if pd.isna(url): return ""
    match = re.search(r'(\d{4}\.\d{4,5})', str(url))
    return match.group(1) if match else ""

# The target counts
check = [1344, 1287, 1585, 1467, 1386, 1478, 1816, 1304, 1860, 1889, 1551, 1305]
months = [f"2025-{i:02d}" for i in range(1, 13)]

print("Comparing CSV counts to ArXiv Target:")
counts = data['date'].str[:7].value_counts().sort_index()

for i, m in enumerate(months):
    actual = counts.get(m, 0)
    target = check[i]
    diff = actual - target
    print(f"{m}: Actual={actual}, Target={target}, Diff={diff}")
