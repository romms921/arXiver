import arxiv
import json
import pandas as pd
import urllib.request as libreq
import certifi
import os
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
load_dotenv()
from datetime import datetime, timedelta
from tqdm import tqdm
from IPython.display import display, Latex
from bs4 import BeautifulSoup
import re
import PyPDF2
import io
os.environ['SSL_CERT_FILE'] = certifi.where()



class ArxivDataframe:
    def __init__(self, subject):
        self.subject = subject.lower()
        self.client = arxiv.Client()
        self.bs4_client = BeautifulSoup
    
    def _retrieve_html(self):
        base_url = f'https://arxiv.org/list/{self.subject}/new'
        page = libreq.urlopen(base_url)
        html = page.read().decode('utf-8')
        return html
    
    def _remove_brackets(self, text):
        """Remove content within brackets from text"""
        return re.sub(r'\(.*?\)', '', text).strip()
    
    def _clean_subjects(self, df):
        """Clean primary and secondary subjects"""
        df['primary_subject'] = df['primary_subject'].map(self._remove_brackets)
        df['secondary_subjects'] = df['secondary_subjects'].map(
            lambda x: [self._remove_brackets(subject) for subject in x] if isinstance(x, list) else x,
            na_action='ignore'
        )
        return df
    
    def _clean_journal(self, df):
        """Clean journal information"""
        df['submitted_journal'] = df['submitted_journal'].str.split(r'[,;:.]').str[0]
        return df
    
    def _extract_affiliations(self, pdf_reader, authors, max_pages=2):
        """
        Extract author affiliations from PDF using a simplified approach with better logging
        """
        print("\n=== Starting Affiliation Extraction ===")
        print(f"Processing authors: {authors}")
        
        affiliations = [None] * len(authors)
        
        try:
            # Get text from first pages
            full_text = ""
            for page_num in range(min(max_pages, len(pdf_reader.pages))):
                try:
                    page_text = pdf_reader.pages[page_num].extract_text()
                    full_text += page_text + "\n"
                    print(f"Successfully read page {page_num + 1}")
                except Exception as e:
                    print(f"Error reading page {page_num + 1}: {str(e)}")
                    continue

            # Clean text
            full_text = re.sub(r'\s+', ' ', full_text)
            
            # Truncate text at common section markers
            section_markers = ['Abstract', 'Introduction', 'Keywords', 'I.', '1.', 'Methods']
            for marker in section_markers:
                pos = full_text.find(marker)
                if pos != -1:
                    full_text = full_text[:pos]
                    print(f"Truncated text at marker: {marker}")
            
            print("\nLooking for affiliation blocks...")
            
            # Simple pattern to find potential affiliation blocks
            affiliation_patterns = [
                # Look for institutional addresses
                r'(?i)(?:Department|University|Institute|Laboratory|School|Center|Centre)[^.]*(?:[^.]*(?:University|Institute|Laboratory|School|Center|Centre)[^.]*)*\.',
                # Look for locations
                r'(?i)(?:[A-Z][a-zA-Z\s]+,\s*[A-Z][a-zA-Z\s]+(?:\s*\d{5})?[^.]*)\.',
                # Look for email domains
                r'(?i)(?:[^.]*@[^.]+\.[^.]+[^.]*)\.'
            ]
            
            potential_affiliations = []
            for pattern in affiliation_patterns:
                matches = re.finditer(pattern, full_text)
                for match in matches:
                    aff = match.group(0).strip()
                    if len(aff) > 20:  # Filter out very short matches
                        potential_affiliations.append(aff)
                        print(f"Found potential affiliation: {aff}")
            
            # Remove duplicates while preserving order
            potential_affiliations = list(dict.fromkeys(potential_affiliations))
            
            print(f"\nFound {len(potential_affiliations)} unique potential affiliations")
            
            # For each author, try to find their affiliation
            for i, author in enumerate(authors):
                try:
                    author_name = author.split()[-1]  # Get last name
                    print(f"\nProcessing author: {author} (searching for: {author_name})")
                    
                    # Look for affiliations near author name
                    author_pos = full_text.find(author)
                    if author_pos != -1:
                        # Look at text chunk around author mention
                        window = 500  # Increased window size
                        start = max(0, author_pos - window//2)
                        end = min(len(full_text), author_pos + window//2)
                        nearby_text = full_text[start:end]
                        
                        author_affiliations = []
                        for aff in potential_affiliations:
                            if aff in nearby_text:
                                author_affiliations.append(aff)
                                print(f"Found matching affiliation: {aff}")
                        
                        if author_affiliations:
                            affiliations[i] = author_affiliations
                        else:
                            print(f"No affiliations found near author {author}")
                    else:
                        print(f"Could not find author {author} in text")
                
                except Exception as e:
                    print(f"Error processing author {author}: {str(e)}")
                    continue
            
            print("\n=== Affiliation Extraction Complete ===")
            print(f"Final affiliations: {affiliations}")
            return affiliations
            
        except Exception as e:
            print(f"Error in affiliation extraction: {str(e)}")
            return [None] * len(authors)
        
    def _extract_pdf_metrics(self, pdf_reader):
        """Extract metrics (pages, figures, tables) from PDF"""
        metrics = {
            'pages': len(pdf_reader.pages),
            'figures': 0,
            'tables': 0
        }
        
        for page in pdf_reader.pages:
            text = page.extract_text()
            # Find figures
            figure_numbers = re.findall(r'(?i)(?:Figure|Fig.|Figure.|Fig})\s+(\d+)', text)
            if figure_numbers:
                metrics['figures'] = max(metrics['figures'], max(map(int, figure_numbers)))
            
            # Find tables
            table_numbers = re.findall(r'(?i)(?:Table|Table.})\s+(\d+)', text)
            if table_numbers:
                metrics['tables'] = max(metrics['tables'], max(map(int, table_numbers)))
                
        return metrics
    
    def _process_pdf(self, pdf_link, current_metrics=None, authors=None):
        """Process PDF to extract metrics, keywords, and affiliations"""
        try:
            pdf_response = libreq.urlopen('https://' + pdf_link)
            pdf_file = pdf_response.read()
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_file))
            
            # Extract metrics if needed
            metrics = self._extract_pdf_metrics(pdf_reader)
            
            # Only update metrics that are currently NaN
            if current_metrics:
                for key in metrics:
                    if pd.isna(current_metrics[key]):
                        current_metrics[key] = metrics[key]
                metrics = current_metrics
            
            # Extract keywords
            keywords = self._extract_keywords(pdf_reader)
            
            # Extract affiliations if authors are provided
            affiliations = None
            if authors:
                affiliations = self._extract_affiliations(pdf_reader, authors)
            
            return {**metrics, 'keywords': keywords, 'affiliations': affiliations}
            
        except Exception as e:
            print(f"Error processing PDF {pdf_link}: {str(e)}")
            return None
        
    def _extract_keywords(self, pdf_reader, max_pages=5):
        """
        Extract keywords from PDF with improved accuracy and efficiency across subjects
        Args:
            pdf_reader: PyPDF2.PdfReader object
            max_pages: Maximum number of pages to search (default: 5, as keywords are usually at the start)
        Returns:
            list: Extracted keywords
        """
        keywords = []
        patterns = [
            r'(?i)(?:key[ -]?words?|index terms)[:.]?\s*(.*?)(?:[.;]|\n|(?=\n\n)|$)',
            r'(?i)(?:PACS numbers?|Mathematics Subject Classification|AMS subject classifications?'
            r'|Computing Classification System|ACM CCS|MeSH terms)[:.]?\s*(.*?)(?:[.;]|\n|(?=\n\n)|$)',
            r'(?i)(?:subject headings?|thesaurus terms?|subject terms?|descriptors?)[:.]?\s*(.*?)(?:[.;]|\n|(?=\n\n)|$)',
            r'(?i)(?:mots[- ]?cl[ée]s?|schlüsselwörter|palabras[- ]?clave)[:.]?\s*(.*?)(?:[.;]|\n|(?=\n\n)|$)'
        ]

        # Common section headers that indicate the end of front matter
        section_markers = [
            '1. Introduction', '1 Introduction', 'Introduction', 
            'Background', 'Literature Review', 'Methods',
            'Methodology', 'Results', 'Discussion',
            'I. ', 'II. ', 'Section 1', 'Section 2'
        ]
        
        try:
            # Only search first few pages for efficiency
            pages_to_search = min(max_pages, len(pdf_reader.pages))          
            for page_num in range(pages_to_search):
                try:
                    text = pdf_reader.pages[page_num].extract_text()
                    if not text:
                        continue
                        
                    # Clean text while preserving important separators
                    text = re.sub(r'\s+', ' ', text)
                    text = re.sub(r'(?<=[.,;])\s*(?=[A-Z])', '\n', text)  # Add breaks at major punctuation
                    
                    # Check for section markers and truncate text
                    for marker in section_markers:
                        marker_pos = text.find(marker)
                        if marker_pos != -1:
                            text = text[:marker_pos]
                            break
                    
                    # Extract keywords using patterns
                    for pattern in patterns:
                        matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
                        for match in matches:
                            # Handle both string and tuple matches
                            match_text = match[0] if isinstance(match, tuple) else match
                            
                            # Clean and split the matched text
                            cleaned_keywords = match_text.strip()
                            # Split on common keyword separators
                            for separator in [';', ',', '•', '·', '—', '-', '\n']:
                                if separator in cleaned_keywords:
                                    keywords.extend([k.strip() for k in cleaned_keywords.split(separator)])
                                    break
                            else:
                                keywords.append(cleaned_keywords)
                            
                except Exception as e:
                    print(f"Error processing page {page_num}: {str(e)}")
                    continue
                
            # Post-processing of keywords
            processed_keywords = []
            for keyword in keywords:
                # Skip if too short or too long
                if not keyword or len(keyword) < 3 or len(keyword) > 100:
                    continue
                # Clean up the keyword
                cleaned = re.sub(r'^\W+|\W+$', '', keyword)  # Remove leading/trailing non-word chars
                cleaned = re.sub(r'\s+', ' ', cleaned)       # Normalize whitespace
                cleaned = cleaned.strip()               
                if cleaned and len(cleaned) >= 3:
                    processed_keywords.append(cleaned)
            
            # Remove duplicates while preserving order
            seen = set()
            final_keywords = []
            for keyword in processed_keywords:
                lower_keyword = keyword.lower()
                if lower_keyword not in seen:
                    seen.add(lower_keyword)
                    final_keywords.append(keyword)
            
            return final_keywords[:10]  # Limit to top 10 keywords
            
        except Exception as e:
            print(f"Error in keyword extraction: {str(e)}")
            return []
    
    def _metadata(self, xml_part):
        soup = self.bs4_client(xml_part, 'html.parser')
        title_tag = soup.find('div', class_='list-title mathjax')
        title = title_tag.get_text(strip=True).replace('Title:', '').strip() if title_tag else None

        # abstract
        abstract_tag = soup.find('p', class_='mathjax')
        abstract = abstract_tag.get_text(strip=True) if abstract_tag else None

        # authors
        authors_section = soup.find('div', class_='list-authors')
        authors = [author.get_text(strip=True) for author in authors_section.find_all('a')] if authors_section else []

        # comments
        comments_tag = soup.find('div', class_='list-comments mathjax')
        comments = comments_tag.get_text(strip=True).replace('Comments:', '').strip() if comments_tag else ''
        
        # figures, pages, tables
        figures_match = re.search(r'(\d+)\s+figures', comments)
        figures = int(figures_match.group(1)) if figures_match else None
        pages_match = re.search(r'(\d+)\s+pages', comments)
        pages = int(pages_match.group(1)) if pages_match else None
        tables_match = re.search(r'(\d+)\s+table[s]?', comments)
        tables = int(tables_match.group(1)) if tables_match else None

        # PDF link
        pdf_tag = soup.find('a', title='Download PDF')
        pdf_link = pdf_tag['href'] if pdf_tag else None

        # primary subject
        primary_subject_tag = soup.find('span', class_='primary-subject')
        primary_subject = primary_subject_tag.get_text(strip=True) if primary_subject_tag else None

        # secondary subjects
        subjects_section = soup.find('div', class_='list-subjects')
        if subjects_section:
            subjects_text = subjects_section.get_text(strip=True)
            subjects_split = subjects_text.split(';')
            secondary_subjects = [subject.strip() for subject in subjects_split[1:]] if len(subjects_split) > 1 else None
        else:
            secondary_subjects = None

        # journal
        submitted_journal = None
        if comments:
            for prefix in ['Submitted to ', 'Accepted to ', 'Accepted for publication in ', 'Accepted by ', 'Submitted by ']:
                if prefix in comments:
                    submitted_journal = comments.split(prefix)[-1]
                    break

        # published
        published_tag = soup.find('div', class_='list-journal-ref')
        published_journal = published_tag.get_text(strip=True).replace('Journal-ref:', '').strip() if published_tag else None

        return {
            'title': title,
            'abstract': abstract,
            'authors': authors,
            'figures': figures,
            'pages': pages,
            'tables': tables,
            'pdf_link': f'arxiv.org{pdf_link}' if pdf_link else None,
            'primary_subject': primary_subject,
            'secondary_subjects': secondary_subjects,
            'submitted_journal': submitted_journal,
            'published_journal': published_journal
        }
    
    def process_dataframe(self, df):
        """Process the dataframe to add all additional features"""
        # Clean subjects and journal information
        df = self._clean_subjects(df)
        df = self._clean_journal(df)
        
        # Initialize keywords column
        df['keywords'] = None
        df['affiliations'] = None
    
        # Process each paper
        for i in tqdm(range(len(df)), desc='Processing PDFs, for metrics, keywords and affiliations'):
            current_metrics = {
                'pages': df['pages'][i],
                'figures': df['figures'][i],
                'tables': df['tables'][i]
            }
            
            # Only process PDF if we're missing any data
            if (pd.isna(current_metrics['pages']) or 
                pd.isna(current_metrics['figures']) or 
                pd.isna(current_metrics['tables']) or 
                pd.isna(df['keywords'][i]) or
                pd.isna(df['affiliations'][i])):
                
                pdf_data = self._process_pdf(
                    df['pdf_link'][i], 
                    current_metrics,
                    authors=df['authors'][i] if 'authors' in df else None
                )
                
                if pdf_data:
                    df.at[i, 'pages'] = pdf_data['pages']
                    df.at[i, 'figures'] = pdf_data['figures']
                    df.at[i, 'tables'] = pdf_data['tables']
                    df.at[i, 'keywords'] = pdf_data['keywords']
                    if pdf_data['affiliations']:
                        df.at[i, 'affiliations'] = pdf_data['affiliations']
        
        return df
    
    def construct_dataframe(self):
        """Construct and process the complete dataframe"""
        # Get initial data
        html = self._retrieve_html()
        soup = self.bs4_client(html, 'html.parser')
        
        h3_tag = soup.find('h3', string=lambda x: x and 'New submissions' in x)
        
        if not h3_tag:
            print("New submissions header not found")
            return pd.DataFrame()
            
        try:
            number_of_papers = int(h3_tag.string.split('(')[1].split()[1])
            print(f"Number of papers: {number_of_papers}")
        except (IndexError, ValueError):
            print("Could not extract number of papers")
            return pd.DataFrame()
            
        # Get metadata for all papers
        items = soup.find_all('a', attrs={'name': True})
        if not items:
            print("No paper items found")
            return pd.DataFrame()
            
        all_metadata = []
        
        # Process papers except the last one
        for i in tqdm(range(number_of_papers-1),desc='Processing Papers'):
            start = items[i]
            end = items[i + 1]
            start_index = str(soup).find(str(start))
            end_index = str(soup).find(str(end))
            xml_part = str(soup)[start_index:end_index]
            metadata = self._metadata(xml_part)
            all_metadata.append(metadata)
            
        # Process the last paper
        last_item = items[-1]
        start_index = str(soup).find(str(last_item))
        xml_part = str(soup)[start_index:]
        metadata = self._metadata(xml_part)
        all_metadata.append(metadata)
        
        # Create and process dataframe
        df = pd.DataFrame(all_metadata) 
        return self.process_dataframe(df)