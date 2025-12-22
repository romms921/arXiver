import streamlit as st
import pandas as pd
import ast
import os

# Set page config for a wider layout
st.set_page_config(layout="wide", page_title="ArXiv Paper Affiliation Editor")

CSV_PATH = "/Users/ainsleylewis/Documents/Astronomy/arXiver/test.csv"

@st.cache_data
def load_data():
    return pd.read_csv(CSV_PATH)

def save_data(df):
    df.to_csv(CSV_PATH, index=False)
    # Clear cache so the UI reflects changes on reload
    st.cache_data.clear()

# Load data
df = load_data()

# Session state for tracking the current paper index
if 'paper_index' not in st.session_state:
    st.session_state.paper_index = 0

# Sidebar Navigation
st.sidebar.header("Navigation")
st.session_state.paper_index = st.sidebar.number_input(
    "Paper Index", 
    min_value=0, 
    max_value=len(df)-1, 
    value=st.session_state.paper_index
)

col_nav1, col_nav2 = st.sidebar.columns(2)
if col_nav1.button("⬅️ Previous"):
    if st.session_state.paper_index > 0:
        st.session_state.paper_index -= 1
        st.rerun()

if col_nav2.button("Next ➡️"):
    if st.session_state.paper_index < len(df) - 1:
        st.session_state.paper_index += 1
        st.rerun()

# Get current paper data
row = df.iloc[st.session_state.paper_index]

def parse_list(val):
    if pd.isna(val):
        return []
    if isinstance(val, str):
        try:
            return ast.literal_eval(val)
        except:
            return [val]
    return val

authors = parse_list(row['authors'])
affiliations = parse_list(row['affiliations'])
title = row['title']
abstract = row['abstract']
pdf_url = row['pdf_link']

if not pdf_url.startswith('http'):
    pdf_url = 'https://' + pdf_url

st.title(f"Paper {st.session_state.paper_index}: {title}")

# Top level summary
st.write(f"**Authors:** {', '.join([str(a) for a in authors])}")

# Layout: 2 columns for Editor and PDF
col_editor, col_pdf = st.columns([1, 1])

with col_editor:
    st.subheader("Affiliation Editor")
    
    # Check if None is present
    has_none = any(a is None or a == "None" for a in affiliations) if isinstance(affiliations, list) else (affiliations is None or affiliations == "None")
    
    if has_none:
        st.warning("⚠️ Missing affiliations (None) detected for this paper.")
    
    # Prepare current affiliations as a string for the text box
    if isinstance(affiliations, list):
        default_aff_str = " : ".join([str(a) if a is not None else "" for a in affiliations])
    else:
        default_aff_str = str(affiliations) if affiliations is not None else ""
        
    st.write(f"Number of authors: **{len(authors)}**")
    new_affs_input = st.text_area(
        "Enter affiliations (separated by a colon ':')", 
        value=default_aff_str, 
        height=150,
        help="Example: Univ A : Univ B : Univ C"
    )
    
    if st.button("Submit and Save to CSV", type="primary"):
        # Process the input
        new_affs_list = [s.strip() for s in new_affs_input.split(":")]
        
        # Update the dataframe
        df.at[st.session_state.paper_index, 'affiliations'] = str(new_affs_list)
        
        # Save to CSV
        save_data(df)
        st.success("✅ Affiliations saved successfully!")
        st.rerun()

    st.markdown("---")
    st.subheader("Abstract")
    st.write(abstract)

with col_pdf:
    st.subheader("PDF Preview")
    st.markdown(f"[Open PDF in new tab]({pdf_url})")
    # Iframe for embedding. Note: Some browsers/sites block this.
    st.components.v1.iframe(pdf_url, height=900, scrolling=True)

st.divider()
st.info(f"Currently viewing record {st.session_state.paper_index + 1} of {len(df)}.")