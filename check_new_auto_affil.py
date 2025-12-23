import pandas as pd
import ast

CSV_PATH = "test_with_affils.csv"
COL = "affiliations_auto"


def inspect_row(idx, row):
    print(f"\n========== ROW {idx} ==========")

    raw = row.get(COL, "")

    if not isinstance(raw, str) or not raw.strip():
        print("❌ No affiliations_auto data")
        return

    try:
        data = ast.literal_eval(raw)
    except Exception as e:
        print("❌ Failed to parse affiliations_auto")
        print("Error:", e)
        print("Raw value:", raw[:200])
        return

    if not isinstance(data, list):
        print("❌ Parsed object is not a list")
        print(type(data))
        return

    for entry in data:
        if not isinstance(entry, dict):
            print("❌ Malformed entry:", entry)
            continue

        author = entry.get("author", "<missing>")
        affils = entry.get("affiliations", [])

        if not affils:
            affils = ["<NO AFFILIATIONS FOUND>"]

        print(f"Author: {author}")
        for a in affils:
            print(f"  - {a}")


def main():
    df = pd.read_csv(CSV_PATH)

    print(f"Loaded {len(df)} rows")

    # Only inspect rows that were processed
    if "processed" in df.columns:
        df = df[df["processed"] == True]

    # Inspect first N rows
    for idx, row in df.head(10).iterrows():
        inspect_row(idx, row)


if __name__ == "__main__":
    main()
