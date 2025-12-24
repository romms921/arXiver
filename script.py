import csv
import sys
import re
from collections import defaultdict

def clean_text(text):
    """Removes ORCID numbers and LaTeX tildes for a cleaner name/affiliation."""
    if not text:
        return ""
    # Remove ORCID patterns (0000-0000-0000-0000)
    text = re.sub(r'\d{4}-\d{4}-\d{4}-\d{3}[\dX]', '', text)
    # Replace LaTeX non-breaking space (~) with a standard space
    text = text.replace('~', ' ')
    # Clean up multiple spaces
    return ' '.join(text.split()).strip()

def format_papers_to_csv(input_file, output_file):
    papers = defaultdict(list)
    
    try:
        # 1. Read and Group the data
        with open(input_file, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or len(row) < 2:
                    continue
                
                title = clean_text(row[0])
                author = clean_text(row[1])
                affiliation = clean_text(row[2]) if len(row) > 2 else ""
                
                # Format: "Name: Affiliation"
                if affiliation:
                    entry = f"{author}: {affiliation}"
                else:
                    entry = f"{author}:"
                
                papers[title].append(entry)

        # 2. Write to the new CSV file
        with open(output_file, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            
            for title, author_list in papers.items():
                all_authors_string = "; ".join(author_list)
                
                # Matching your specific format: Title, Bibcode, Combined Authors, Empty
                writer.writerow([
                    title,
                    "BIBCODE_PLACEHOLDER", 
                    all_authors_string,
                    ""
                ])
                
        print(f"Successfully saved formatted data to: {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Usage: python script.py input.csv output.csv
    if len(sys.argv) < 3:
        print("Usage: python script.py <input_csv> <output_csv>")
    else:
        format_papers_to_csv(sys.argv[1], sys.argv[2])