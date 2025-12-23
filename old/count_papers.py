import pandas as pd
data = pd.read_csv("FINAL_ARXIV_2025.csv")
data['date'] = pd.to_datetime(data['date'])
monthly_counts = (
    data['date']
    .dt.to_period('M')
    .value_counts()
    .sort_index()
)
print(monthly_counts)

data1 = pd.read_csv("missing_papers_stats.csv")
data1['Missing_Count'] = pd.to_numeric(data1['Missing_Count'], errors='coerce')
#sum the missing count column
print(data1['Missing_Count'].sum())
print(data1['Missing_Count'].sum() + len(data))
