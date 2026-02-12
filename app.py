import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

st.set_page_config(page_title="E-commerce Behavioral Analytics", layout="wide")

def load_data():
    conn = sqlite3.connect('data/ecommerce_analytics.db')
    query = "SELECT * FROM user_behavior"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.title("Behavioral Dashboard")
st.markdown("""
This dashboard simulates a **Solutions Engineer** tool to monitor user behavior patterns and identify potential "at-risk" interactions based on activity frequency.
""")

try:
    df = load_data()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Events", len(df))
    with col2:
        unique_users = df['user_id'].nunique()
        st.metric("Unique Users", unique_users)
    with col3:
        high_freq_events = df['is_high_frequency'].sum()
        st.metric("High Frequency Actions", int(high_freq_events), delta="Pattern Detected", delta_color="inverse")

    # --- Charts ---
    st.divider()
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Actions Distribution")
        fig_actions = px.pie(df, names='action', hole=0.4, color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig_actions, width='stretch')

    with c2:
        st.subheader("Events by Category")
        fig_cat = px.bar(df['category'].value_counts().reset_index(), x='category', y='count', color='category')
        st.plotly_chart(fig_cat, width='stretch')

    # --- Flagging Risks ---
    st.divider()
    st.subheader("High-Frequency Activity Logs")
    st.info("The table below filters users who performed actions in less than 2 seconds (Potential Loss of Control).")
    
    # Filtering the data for the 'impulsive' flag
    risk_df = df[df['is_high_frequency'] == 1][['user_id', 'timestamp', 'action', 'time_diff']]
    st.dataframe(risk_df, width='stretch')

except Exception as e:
    st.error(f"Error: {e}. Please ensure you have run 'python src/processor.py' first.")