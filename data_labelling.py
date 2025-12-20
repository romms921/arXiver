import streamlit as st
import pandas as pd
import os
import ast
import json
import re

# Set page config for a premium, wide layout
st.set_page_config(
    page_title="ArXiver | Data Labelling",
    page_icon="📝",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for a premium, dark-themed aesthetic
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&family=Outfit:wght@300;400;700&display=swap');

    :root {
        --primary-gradient: linear-gradient(135deg, #6366f1 0%, #a855f7 100%);
        --glass-bg: rgba(255, 255, 255, 0.03);
        --glass-border: rgba(255, 255, 255, 0.1);
    }

    .main {
        background-color: #0f172a;
        color: #f8fafc;
        font-family: 'Inter', sans-serif;
    }

    h1, h2, h3 {
        font-family: 'Outfit', sans-serif;
        background: var(--primary-gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
    }

    .stButton>button {
        background: var(--primary-gradient);
        color: white;
        border: none;
        padding: 0.5rem 1.5rem;
        border-radius: 12px;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(99, 102, 241, 0.3);
    }

    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(99, 102, 241, 0.4);
        color: white;
    }

    .stTextInput>div>div>input {
        background: var(--glass-bg);
        border: 1px solid var(--glass-border);
        color: #f8fafc;
        border-radius: 12px;
        padding: 0.75rem;
    }

    .paper-card {
        background: var(--glass-bg);
        border: 1px solid var(--glass-border);
        border-radius: 20px;
        padding: 2.5rem;
        backdrop-filter: blur(10px);
        margin-bottom: 2rem;
        box-shadow: 0 10px 30px rgba(0,0,0,0.5);
        min-height: 400px;
        display: flex;
        flex-direction: column;
    }

    .keyword-tag {
        display: inline-block;
        background: rgba(99, 102, 241, 0.1);
        border: 1px solid rgba(99, 102, 241, 0.3);
        color: #818cf8;
        padding: 0.2rem 0.8rem;
        border-radius: 20px;
        margin: 0.3rem;
        font-size: 0.85rem;
        font-weight: 600;
    }

    .nav-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        background: var(--glass-bg);
        padding: 1rem 2rem;
        border-radius: 15px;
        border: 1px solid var(--glass-border);
        margin-top: 2rem;
    }

    .no-kw-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(60px, 1fr));
        gap: 8px;
        margin-top: 1rem;
        max-height: 400px;
        overflow-y: auto;
        padding: 1rem;
        background: rgba(0,0,0,0.2);
        border-radius: 12px;
    }

    .no-kw-item {
        background: var(--glass-bg);
        border: 1px solid var(--glass-border);
        color: #818cf8;
        text-align: center;
        padding: 4px;
        border-radius: 6px;
        font-size: 0.8rem;
        cursor: pointer;
        transition: all 0.2s;
    }

    .no-kw-item:hover {
        background: #818cf8;
        color: white;
    }

    /* Hide Streamlit Header/Footer */
    header {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

CSV_PATH = 'arxiv_papers_copy.csv'

# Load Data
@st.cache_data
def get_data():
    if not os.path.exists(CSV_PATH):
        st.error(f"File {CSV_PATH} not found.")
        return pd.DataFrame()
    return pd.read_csv(CSV_PATH)

@st.cache_data
def get_keyword_bank():
    json_path = os.path.join(os.path.dirname(__file__), 'uat.json')
    if not os.path.exists(json_path):
        # Fallback if not found, or warn
        return []

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    keywords = set()
    # Keys for labels in UAT (SKOS/RDF)
    label_keys = [
        "http://www.w3.org/2004/02/skos/core#prefLabel",
        "http://www.w3.org/2004/02/skos/core#altLabel",
        "http://www.w3.org/2000/01/rdf-schema#label"
    ]

    for concept_uri, properties in data.items():
        for key in label_keys:
            if key in properties:
                for item in properties[key]:
                    # User requirement: filter "value" field where "type" : "literal"
                    if isinstance(item, dict) and item.get("type") == "literal":
                        val = item.get("value")
                        if val:
                            keywords.add(val)
    
    return list(keywords)

def recommend_keywords(title, abstract, bank):
    # Combine title and abstract
    text = f"{str(title)} {str(abstract)}".lower()
    
    # Extract unique tokens from text (bag of words)
    # Using regex to match alphanumeric words, effectively ignoring punctuation
    text_tokens = set(re.findall(r'\b\w+\b', text))
    
    matches = []
    # Check each keyword
    for kw in bank:
        # Tokenize the keyword phrase
        kw_tokens = re.findall(r'\b\w+\b', kw.lower())
        
        if not kw_tokens:
            continue
            
        # Optimization: Fast check with sets
        # If all tokens in the keyword phrase appear in the text tokens (regardless of order)
        if set(kw_tokens).issubset(text_tokens):
            matches.append(kw)
            
    # Return all unique matches, no limit
    return sorted(list(set(matches)))

# We use session state to track index to avoid losing place on rerun
if 'current_idx' not in st.session_state:
    st.session_state.current_idx = 0

# Check query params for navigation
query_params = st.query_params
if "index" in query_params:
    try:
        new_idx = int(query_params["index"])
        st.session_state.current_idx = new_idx
    except:
        pass

df = get_data()

if df.empty:
    st.stop()

# Helper to save data back to CSV
def save_data(index, new_keywords):
    # Load fresh data to avoid overwriting other potential changes
    # and to ensure we are modifying the right row in case of concurrent edits (unlikely here but good practice)
    full_df = pd.read_csv(CSV_PATH)
    full_df.at[index, 'keywords'] = new_keywords
    full_df.to_csv(CSV_PATH, index=False)
    st.toast("✅ Saved successfully!", icon="🚀")
    # Clear cache to ensure next reload sees the change
    st.cache_data.clear()

# UI Layout
st.markdown("<h1>ArXiver <span style='color:#f8fafc; opacity: 0.5;'>| Data Labelling</span></h1>", unsafe_allow_html=True)

# Main Dashboard
col1, col2 = st.columns([3, 1])

with col1:
    idx = st.session_state.current_idx
    paper = df.iloc[idx]
    
    st.markdown(f"""
    <div class="paper-card">
        <h2 style='margin-top:0;'>{paper['title']}</h2>
        <p style='opacity: 0.7; font-size: 0.9rem; margin-bottom: 1.5rem;'>
            <b>Authors:</b> {paper['authors']}<br>
            <b>Published:</b> {paper['date']} | <b>Subject:</b> {paper['primary_subject']}
        </p>
        <div style='background: rgba(0,0,0,0.2); padding: 1.5rem; border-radius: 12px; margin-bottom: 2rem;'>
            <h4 style='margin-top:0; color: #818cf8;'>Abstract</h4>
            <p style='font-size: 0.95rem; line-height: 1.6;'>{paper['abstract']}</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Recommendations Box
    bank = get_keyword_bank()
    recommendations = recommend_keywords(paper['title'], paper['abstract'], bank)
    
    if recommendations:
        st.markdown(f"""
        <div style='background: linear-gradient(145deg, rgba(99, 102, 241, 0.1), rgba(168, 85, 247, 0.1)); 
                    border: 1px solid rgba(168, 85, 247, 0.2); 
                    padding: 1.5rem; 
                    border-radius: 16px; 
                    margin-bottom: 2rem;
                    box-shadow: 0 4px 20px rgba(0,0,0,0.2);'>
            <div style='display: flex; align-items: center; gap: 10px; margin-bottom: 1rem;'>
                <span style='font-size: 1.2rem;'>💡</span>
                <h5 style='margin:0; font-size: 1rem; color: #ffffff; font-weight: 600;'>Smart Recommendations</h5>
            </div>
            <p style='font-size: 0.8rem; color: #ccc; margin-top: 0; margin-bottom: 0;'>
                Click a tag to add it:
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Interactive Buttons
        # Use a grid layout for buttons
        rec_cols = st.columns(3)
        kw_key = f"kw_{st.session_state.current_idx}"
        
        for i, rec in enumerate(recommendations):
            # Styling hack for buttons can be done via Global CSS, or just standard buttons
            # We use columns to pack them slightly better
            if rec_cols[i % 3].button(f"➕ {rec}", key=f"btn_rec_{i}", use_container_width=True):
                # Logic to add keyword
                # 1. Initialize state if missing (before text area renders)
                if kw_key not in st.session_state:
                    st.session_state[kw_key] = paper['keywords']
                
                # 2. Parse current list
                current_str = st.session_state[kw_key]
                try:
                    # Handle potential non-string or nan
                    if pd.isna(current_str):
                        current_list = []
                    else:
                        current_list = ast.literal_eval(str(current_str))
                        if not isinstance(current_list, list):
                            current_list = []
                except:
                    current_list = []
                
                # 3. Add if new
                if rec not in current_list:
                    current_list.append(rec)
                    st.session_state[kw_key] = str(current_list)
                    # Force rerun to show updated text area immediately (optional, but safer for UI sync)
                    st.rerun()
                else:
                    st.toast(f"'{rec}' is already in the list!", icon="ℹ️")



with col2:
    st.markdown("### 🏷️ Keywords")
    
    # Process keywords string to list
    try:
        current_keywords_list = ast.literal_eval(paper['keywords'])
    except:
        current_keywords_list = []
    
    # Display current keywords as tags
    st.markdown("<div style='margin-bottom: 2rem;'>", unsafe_allow_html=True)
    for k in current_keywords_list:
        st.markdown(f'<span class="keyword-tag">{k}</span>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Input for editing/adding
    # We use a key based on index to reset the input when moving papers
    new_kw_str = st.text_area("Edit Keywords (Python list format)", value=paper['keywords'], height=150, key=f"kw_{idx}")
    
    if st.button("✨ Done", use_container_width=True):
        save_data(idx, new_kw_str)
        # Move to next automatically
        if st.session_state.current_idx < len(df) - 1:
            st.session_state.current_idx += 1
            st.rerun()

# Navigation Bar
st.markdown("<div class='nav-container'>", unsafe_allow_html=True)
nav_col1, nav_col2, nav_col3, nav_col4 = st.columns([1, 1, 2, 1])

with nav_col1:
    if st.button("⬅️ Previous", disabled=(st.session_state.current_idx == 0), use_container_width=True):
        st.session_state.current_idx -= 1
        st.rerun()

with nav_col2:
    if st.button("Next ➡️", disabled=(st.session_state.current_idx == len(df) - 1), use_container_width=True):
        st.session_state.current_idx += 1
        st.rerun()

with nav_col3:
    jump_idx = st.number_input("Jump to Paper #", min_value=0, max_value=len(df)-1, value=st.session_state.current_idx, step=1)
    if jump_idx != st.session_state.current_idx:
        st.session_state.current_idx = jump_idx
        st.rerun()

with nav_col4:
    st.markdown(f"<div style='text-align: right; padding-top: 1.5rem; font-weight: 700; color: #818cf8;'>Paper {st.session_state.current_idx + 1} of {len(df)}</div>", unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)

# JavaScript for Keyboard Navigation
st.markdown("""
<script>
const doc = window.parent.document;
doc.addEventListener('keydown', function(e) {
    if (e.key === 'ArrowLeft') {
        const prevBtn = Array.from(doc.querySelectorAll('button')).find(el => el.innerText.includes('Previous'));
        if (prevBtn) prevBtn.click();
    } else if (e.key === 'ArrowRight') {
        const nextBtn = Array.from(doc.querySelectorAll('button')).find(el => el.innerText.includes('Next'));
        if (nextBtn) nextBtn.click();
    }
});
</script>
""", unsafe_allow_html=True)

# Missing Keywords Section
st.markdown("<br><h3>⚠️ Papers Missing Keywords</h3>", unsafe_allow_html=True)

@st.cache_data
def get_missing_kw_indices(_df):
    indices = []
    for i, row in _df.iterrows():
        try:
            val = row['keywords']
            # Robust check for empty/null values
            if pd.isna(val) or val is None:
                indices.append(i)
                continue
                
            val_str = str(val).strip()
            if val_str == "" or val_str == "[]" or val_str == "['']":
                indices.append(i)
                continue
                
            # Try to parse
            try:
                kw = ast.literal_eval(val_str)
            except:
                # If syntax error, treat as missing/corrupted
                indices.append(i)
                continue
                
            if not isinstance(kw, list):
                # If not a list (e.g. tuple or something else), treat as issue
                indices.append(i)
                continue

            # Check if empty list or list of empty strings
            if len(kw) == 0:
                indices.append(i)
            elif all(isinstance(k, str) and k.strip() == "" for k in kw):
                indices.append(i)

        except:
            indices.append(i)
    return indices

missing_indices = get_missing_kw_indices(df)

st.markdown(f"<p style='opacity:0.7;'>Total papers missing keywords: <b>{len(missing_indices)}</b> (Showing up to 5000)</p>", unsafe_allow_html=True)

# Using a grid display with actual links (more robust than JS hack)
grid_html = '<div class="no-kw-grid">'
for idx_m in missing_indices[:5000]:
    # Use target="_self" to force reload in same tab, which triggers query param check
    grid_html += f'<a class="no-kw-item" href="/?index={idx_m}" target="_self" style="text-decoration:none; display:block;">{idx_m}</a>'
grid_html += '</div>'

st.markdown(grid_html, unsafe_allow_html=True)

# Footer Info
st.markdown("<br><hr style='opacity:0.1'><br>", unsafe_allow_html=True)
st.caption("ArXiver Labelling Tool v1.0. Designed for Astro-ph Research.")
