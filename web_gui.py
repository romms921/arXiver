import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import ast
from collections import Counter

# Set page config
st.set_page_config(
    page_title="arXiver Analytics Dashboard",
    page_icon="📚",
    layout="wide",
)

# Custom CSS for premium look
st.markdown("""
<style>
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background-color: #161b22;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #30363d;
    }
    .stPlotlyChart {
        background-color: #161b22;
        border-radius: 10px;
        border: 1px solid #30363d;
        padding: 10px;
    }
    h1, h2, h3 {
        color: #58a6ff;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    df = pd.read_csv("arxiv_papers_copy.csv")
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    # Cleanup numeric columns
    for col in ['figures', 'pages', 'tables']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Parse list-like columns
    def safe_eval(x):
        try:
            if pd.isna(x): return []
            return ast.literal_eval(x)
        except:
            return []

    df['authors_list'] = df['authors'].apply(safe_eval)
    df['keywords_list'] = df['keywords'].apply(safe_eval)
    df['secondary_subjects_list'] = df['secondary_subjects'].apply(safe_eval)
    
    return df

# Header
st.title("📚 arXiver Statistics")
st.markdown("---")

try:
    df = load_data()
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Date range filter
    min_date = df['date'].min().date()
    max_date = df['date'].max().date()
    start_date, end_date = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Subject filter
    subjects = sorted(df['primary_subject'].unique().tolist())
    selected_subjects = st.sidebar.multiselect("Primary Subjects", subjects, default=[])
    
    # Filter dataframe
    filtered_df = df[
        (df['date'].dt.date >= start_date) & 
        (df['date'].dt.date <= end_date)
    ]
    if selected_subjects:
        filtered_df = filtered_df[filtered_df['primary_subject'].isin(selected_subjects)]

    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Papers", f"{len(filtered_df):,}")
    with col2:
        st.metric("Avg Pages", f"{filtered_df['pages'].mean():.1f}")
    with col3:
        st.metric("Avg Figures", f"{filtered_df['figures'].mean():.1f}")
    with col4:
        st.metric("Avg Tables", f"{filtered_df['tables'].mean():.1f}")

    st.markdown("---")

    # Layout with columns
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("📈 Papers Over Time")
        resample_rule = st.selectbox("Resolution", ["D", "W", "M"], index=2)
        time_data = filtered_df.set_index('date').resample(resample_rule).size().reset_index()
        time_data.columns = ['date', 'count']
        fig_time = px.line(time_data, x='date', y='count', title=f"Publications by {resample_rule}",
                           template="plotly_dark", color_discrete_sequence=['#58a6ff'])
        st.plotly_chart(fig_time, use_container_width=True)

    with c2:
        st.subheader("🎯 Top Primary Subjects")
        subj_counts = filtered_df['primary_subject'].value_counts().head(10).reset_index()
        subj_counts.columns = ['subject', 'count']
        fig_subj = px.bar(subj_counts, x='count', y='subject', orientation='h',
                          title="Top 10 Subjects", template="plotly_dark",
                          color='count', color_continuous_scale='Blues')
        st.plotly_chart(fig_subj, use_container_width=True)

    st.markdown("---")
    
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("👨‍🔬 Top Authors")
        all_authors = [author for sublist in filtered_df['authors_list'] for author in sublist]
        author_counts = pd.Series(all_authors).value_counts().head(15).reset_index()
        author_counts.columns = ['author', 'count']
        fig_authors = px.bar(author_counts, x='count', y='author', orientation='h',
                             title="Top 15 Most Active Authors", template="plotly_dark",
                             color='count', color_continuous_scale='Viridis')
        st.plotly_chart(fig_authors, use_container_width=True)

    with c4:
        st.subheader("🔑 Top Keywords")
        all_keywords = [kw for sublist in filtered_df['keywords_list'] for kw in sublist]
        # Clean keywords a bit
        all_keywords = [kw.split(' (')[0].strip() for kw in all_keywords]
        kw_counts = pd.Series(all_keywords).value_counts().head(15).reset_index()
        kw_counts.columns = ['keyword', 'count']
        fig_kw = px.bar(kw_counts, x='count', y='keyword', orientation='h',
                        title="Top 15 Common Keywords", template="plotly_dark",
                        color='count', color_continuous_scale='Plasma')
        st.plotly_chart(fig_kw, use_container_width=True)

    st.markdown("---")
    
    st.subheader("📊 Distributions")
    dist_col1, dist_col2, dist_col3 = st.columns(3)
    
    with dist_col1:
        fig_pages = px.histogram(filtered_df[filtered_df['pages'] < 50], x='pages', 
                                 title="Page Count Distribution (< 50)", template="plotly_dark")
        st.plotly_chart(fig_pages, use_container_width=True)
    
    with dist_col2:
        fig_figs = px.histogram(filtered_df[filtered_df['figures'] < 30], x='figures', 
                                title="Figures Distribution (< 30)", template="plotly_dark")
        st.plotly_chart(fig_figs, use_container_width=True)
        
    with dist_col3:
        fig_tabs = px.histogram(filtered_df[filtered_df['tables'] < 15], x='tables', 
                                title="Tables Distribution (< 15)", template="plotly_dark")
        st.plotly_chart(fig_tabs, use_container_width=True)

    st.markdown("---")
    
    st.subheader("📄 Recent Papers / Search")
    search_query = st.text_input("Search in titles or abstracts")
    if search_query:
        display_df = filtered_df[
            filtered_df['title'].str.contains(search_query, case=False, na=False) |
            filtered_df['abstract'].str.contains(search_query, case=False, na=False)
        ]
    else:
        display_df = filtered_df

    st.dataframe(
        display_df[['date', 'title', 'primary_subject', 'authors', 'pdf_link']].sort_values('date', ascending=False).head(100),
        use_container_width=True
    )

except Exception as e:
    st.error(f"Error loading CSV file: {e}")
    st.info("Make sure 'arxiv_papers_copy.csv' is in the current directory.")
