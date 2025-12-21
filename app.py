import streamlit as st
import pandas as pd
from engine import download_arxiv_pdf, extract_text_from_stream, suggest_affiliations

st.set_page_config(layout="wide", page_title="ArXiv Affiliation Tool")

# Initialize Session State keys
if 'extracted_text' not in st.session_state: st.session_state.extracted_text = ""
if 'current_suggestions' not in st.session_state: st.session_state.current_suggestions = {}

# --- LOAD DATA ---
CSV_PATH = "FINAL_ARXIV_2025_copy.csv"
if 'data' not in st.session_state:
    try:
        st.session_state.data = pd.read_csv(CSV_PATH)
        if 'final_affiliations' not in st.session_state.data.columns:
            st.session_state.data['final_affiliations'] = ""
    except:
        st.session_state.data = pd.DataFrame({
            'title': ['Sample Paper'], 'authors': ['John Doe'], 'pdf_link': ['https://arxiv.org/pdf/1706.03762.pdf'], 'final_affiliations': [""]
        })

# --- SIDEBAR ---
idx = st.sidebar.number_input("Row Index", min_value=0, max_value=len(st.session_state.data)-1, step=1)

# Clear AI suggestions when changing papers
if 'last_idx' not in st.session_state or st.session_state.last_idx != idx:
    st.session_state.current_suggestions = {}
    st.session_state.extracted_text = ""
    st.session_state.last_idx = idx

row = st.session_state.data.iloc[idx]
st.title(f"Reviewing: {row['title']}")

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("1. Paper Content")
    if st.button("🚀 Fetch PDF & Extract Text"):
        pdf_stream = download_arxiv_pdf(row['pdf_link'])
        if pdf_stream:
            st.session_state.extracted_text = extract_text_from_stream(pdf_stream)
        else:
            st.error("Download failed.")
    
    st.text_area("Header Text", value=st.session_state.extracted_text, height=400)

with col2:
    st.subheader("2. Affiliation Mapping")
    
    if st.button("🤖 Suggest with AI (Gemini)"):
        if st.session_state.extracted_text:
            with st.spinner("Gemini is analyzing..."):
                results = suggest_affiliations(st.session_state.extracted_text, row['authors'])
                if "error" not in results:
                    st.session_state.current_suggestions = results
                    st.rerun() # Force refresh to show results in text boxes
                else:
                    st.error(results["error"])
        else:
            st.warning("Extract text first!")

    # Display Input Fields
    authors = [a.strip() for a in str(row['authors']).split(',')]
    existing_affs = str(row['final_affiliations']).split('; ')
    
    updated_values = []
    for i, author in enumerate(authors):
        # Determine the best value to show
        ai_val = st.session_state.current_suggestions.get(author, "")
        saved_val = existing_affs[i] if i < len(existing_affs) else ""
        
        # Priority: AI Suggestion (if just generated) > Saved Data
        display_val = ai_val if ai_val else saved_val
        
        val = st.text_input(f"Affiliation: {author}", value=display_val, key=f"input_{idx}_{i}")
        updated_values.append(val)
    
    if st.button("✅ Save & Export Row"):
        st.session_state.data.at[idx, 'final_affiliations'] = "; ".join(updated_values)
        st.session_state.data.to_csv(CSV_PATH, index=False)
        st.success("Saved!")

st.divider()
st.dataframe(st.session_state.data.head(10))