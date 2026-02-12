import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Behavioral Analytics Pro", layout="wide")

def load_data():
    """
    Securely loads data using absolute path resolution and context managers.
    """
    try:
        base_dir = Path(__file__).resolve().parent
        db_path = (base_dir / 'data' / 'ecommerce_analytics.db').resolve()

        if not db_path.exists():
            st.error("Database file not found. Please run the ETL pipeline first.")
            return pd.DataFrame()

        with sqlite3.connect(db_path) as conn:
            query = "SELECT * FROM user_behavior"
            df = pd.read_sql(query, conn)
            
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
        return df
        
    except Exception as e:
        st.error("An error occurred while loading application data.")
        return pd.DataFrame()

st.title("Behavioral Risk Intelligence Dashboard")

df = load_data()

if not df.empty:
    try:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Events", len(df))
        m2.metric("Unique Users", df['user_id'].nunique())
        m3.metric("Avg Session (min)", f"{df['session_duration_min'].mean():.1f}")
        
        risk_count = int(df['is_high_risk'].sum())
        m4.metric("High Risk Events", risk_count, delta="Pattern Alert", delta_color="inverse")

        st.divider()
        
        c1, c2 = st.columns(2)
        with c1:
            fig_pie = px.pie(df, names='action', hole=0.4, title="Action Mix (Impulsivity)")
            st.plotly_chart(fig_pie, width='stretch')
        
        with c2:
            fig_hour = px.histogram(
                df, x='hour', nbins=24, 
                title="Activity by Hour (Night Watch)", 
                color_discrete_sequence=['#FF4B4B'],
                labels={'hour': 'Hour of Day', 'count': 'Event Count'}
            )
            fig_hour.update_layout(
                bargap=0.2,
                showlegend=False,
                xaxis=dict(tickmode='linear', tick0=0, dtick=2)
            )
            st.plotly_chart(fig_hour, width='stretch')
            
            night_df = df[(df['hour'] >= 0) & (df['hour'] <= 5)]
            night_pct = (len(night_df) / len(df)) * 100 if len(df) > 0 else 0
            
            if night_pct > 20:
                st.warning(f"**Insight:** High nocturnal activity detected ({night_pct:.1f}%). Correlates with potential loss of control.")
            else:
                st.info(f"**Insight:** Nocturnal activity is within normal parameters ({night_pct:.1f}%).")

        st.divider()
        
        st.subheader("Behavioral Risk Profiling")
        col_a, col_b = st.columns(2)
        
        with col_a:
            st.write("**Spending Volatility (Amount Variance)**")
            fig_vol = px.box(
                df[df['amount'] > 0], 
                x='category', y='amount', 
                color='category', 
                title="Spending Spikes by Category"
            )
            st.plotly_chart(fig_vol, width='stretch')
            
            if not df.empty:
                top_volatile = df.sort_values(by='amount_volatility', ascending=False).iloc[0]
                st.info(f"**Insight:** User `{top_volatile['user_id']}` shows peak volatility ({top_volatile['amount_volatility']:.2f}).")

        with col_b:
            st.write("**Session Fatigue Analysis**")
            fig_fatigue = px.scatter(
                df, x='timestamp', y='session_duration_min', 
                color='is_high_risk', 
                title="Session Duration vs Time (Risk Flagged)",
                color_discrete_map={0: "#636EFA", 1: "#EF553B"}
            )
            st.plotly_chart(fig_fatigue, width='stretch')
            
            high_risk_avg = df[df['is_high_risk'] == 1]['session_duration_min'].mean()
            normal_avg = df[df['is_high_risk'] == 0]['session_duration_min'].mean()
            st.error(f"**Insight:** Flagged users average {high_risk_avg:.1f} min/session vs {normal_avg:.1f} min for others.")

        st.divider()
        st.subheader("High-Risk Action Logs")
        risk_cols = ['user_id', 'timestamp', 'action', 'rolling_avg_latency', 'amount_volatility', 'session_duration_min']
        risk_df = df[df['is_high_risk'] == 1][risk_cols]
        
        st.dataframe(
            risk_df.sort_values(by='timestamp', ascending=False), 
            width='stretch', 
            hide_index=True
        )

    except Exception as e:
        st.error("Visualization error. Please check data source integrity.")
else:
    st.warning("No data loaded. Check ETL pipeline logs.")