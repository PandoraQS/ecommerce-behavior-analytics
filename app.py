import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

st.set_page_config(page_title="Behavioral Analytics Pro", layout="wide")

def load_data():
    conn = sqlite3.connect('data/ecommerce_analytics.db')
    df = pd.read_sql("SELECT * FROM user_behavior", conn)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
    conn.close()
    return df

st.title("Behavioral Risk Intelligence Dashboard")

try:
    df = load_data()
    
    # --- Metrics ---
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Events", len(df))
    m2.metric("Unique Users", df['user_id'].nunique())
    m3.metric("Avg Session (min)", f"{df['session_duration_min'].mean():.1f}")
    m4.metric("High Risk Events", int(df['is_high_risk'].sum()), delta="Pattern Alert", delta_color="inverse")

    st.divider()
    
    # --- Row 1: Action Mix & Night Watch ---
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.pie(df, names='action', hole=0.4, title="Action Mix (Impulsivity)"), width='stretch')
    
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
        
        night_events = len(df[(df['hour'] >= 0) & (df['hour'] <= 5)])
        night_pct = (night_events / len(df)) * 100
        if night_pct > 20:
            st.warning(f"**Insight:** High nocturnal activity detected ({night_pct:.1f}%). This correlates with increased loss of control.")
        else:
            st.info(f"**Insight:** Nocturnal activity is within normal parameters ({night_pct:.1f}%).")

    st.divider()
    
    # --- Row 2: Behavioral Risk Profiling ---
    st.subheader("Behavioral Risk Profiling")
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.write("**Spending Volatility (Amount Variance)**")
        fig_vol = px.box(df[df['amount'] > 0], x='category', y='amount', color='category', title="Spending Spikes by Category")
        st.plotly_chart(fig_vol, width='stretch')
        
        top_volatile_user = df.sort_values(by='amount_volatility', ascending=False).iloc[0]
        st.info(f"**Insight:** User `{top_volatile_user['user_id']}` shows the highest spending volatility ({top_volatile_user['amount_volatility']:.2f}). This indicates erratic purchasing patterns.")

    with col_b:
        st.write("**Session Fatigue Analysis**")
        fig_fatigue = px.scatter(df, x='timestamp', y='session_duration_min', color='is_high_risk', 
                                 title="Session Duration vs Time (Risk Flagged)",
                                 color_discrete_map={0: "#636EFA", 1: "#EF553B"})
    
        st.plotly_chart(fig_fatigue, width='stretch')
        
        avg_fatigue = df[df['is_high_risk'] == 1]['session_duration_min'].mean()
        st.error(f"**Insight:** Flagged users have an average session length of {avg_fatigue:.1f} min, compared to {df['session_duration_min'].mean():.1f} min for standard users.")

    # --- Risk Table ---
    st.divider()
    st.subheader("High-Risk Action Logs")
    risk_df = df[df['is_high_risk'] == 1][['user_id', 'timestamp', 'action', 'rolling_avg_latency', 'amount_volatility', 'session_duration_min']]
    st.dataframe(risk_df.sort_values(by='timestamp', ascending=False), width='stretch', hide_index=True)

except Exception as e:
    st.error(f"Error: {e}")