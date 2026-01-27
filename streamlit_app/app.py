import streamlit as st
import clickhouse_connect
import pandas as pd
import os
import plotly.express as px
from datetime import datetime, timedelta

# 1. Page configuration
st.set_page_config(page_title="DB Punctuality Tracker", layout="wide")

# 2. Connection settings
CH_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASS = os.getenv('CLICKHOUSE_PASSWORD')

@st.cache_resource
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=8123, username=CH_USER, password=CH_PASS
    )

client = get_clickhouse_client()

# --- Helper functions ---
def get_available_cities():
    try:
        df = client.query_df("SELECT DISTINCT city FROM train_delays ORDER BY city")
        if not df.empty:
            return df['city'].tolist()
    except Exception as e:
        print(f"Error: {e}")
    return ["Berlin Hbf", "Köln Hbf", "München Hbf"]

def get_available_train_types(city_name):
    try:
        query = f"SELECT DISTINCT train_type FROM train_delays WHERE city = '{city_name}' ORDER BY train_type"
        return client.query_df(query)['train_type'].tolist()
    except:
        return []

# --- UI ---
st.title("🚆 DB Punctuality Index")
st.write("Data is loaded directly from ClickHouse. Only trains with delay > 0 min are shown.")

# --- Sidebar ---
st.sidebar.header("Filters")
available_cities = get_available_cities()
city = st.sidebar.selectbox("Select city", available_cities)

# Get all types
train_types_list = get_available_train_types(city)

# Select ALL types by default
selected_types = st.sidebar.multiselect(
    "Train types", 
    train_types_list, 
    default=train_types_list
)

if not selected_types:
    st.warning("Please select at least one train type.")
    st.stop()

# 4. Main data query
# Fixed syntax for ClickHouse: subtractHours() or INTERVAL
query_analytics = f"""
SELECT
    actual_departure,
    train_type,
    delay_in_min,
    train_id,
    origin,
    destination
FROM train_delays
WHERE city = '{city}' 
  AND actual_departure >= now() - INTERVAL 24 HOUR
  AND train_type IN {tuple(selected_types) if len(selected_types) > 1 else f"('{selected_types[0]}')"}
ORDER BY actual_departure ASC
"""

# === LOAD AND DISPLAY ===
try:
    df_raw = client.query_df(query_analytics)

    if not df_raw.empty:
        # Remove duplicates
        df_raw = df_raw.drop_duplicates(subset=['train_id', 'actual_departure'], keep='first')
        
        # --- FILTERING ---
        df_analytics = df_raw[df_raw['delay_in_min'] > 0].copy()

        if df_analytics.empty:
            st.success(f"No delays found in {city} in the last 24 hours (among selected types).")
            st.stop()
        
        # --- 1. KPI BLOCK (Metrics) ---
        st.subheader("📈 Delay Statistics (24h)")
        
        total_delayed_trains = len(df_analytics)
        avg_delay = df_analytics['delay_in_min'].mean()
        median_delay = df_analytics['delay_in_min'].median()
        max_delay = df_analytics['delay_in_min'].max()
        
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Delayed trains", total_delayed_trains)
        kpi2.metric("Average delay", f"{avg_delay:.1f} min")
        kpi3.metric("Median delay", f"{median_delay:.1f} min")
        kpi4.metric("Maximum delay", f"{max_delay:.0f} min")
        
        st.divider() 

        # --- 2. Scatter plot (Points) ---
        st.subheader(f"📊 Delay Timeline in {city}")
        
        # Convert datetime to pandas datetime for correct Plotly handling
        df_analytics['actual_departure'] = pd.to_datetime(df_analytics['actual_departure'])
        
        fig_scatter = px.scatter(
            df_analytics, 
            x="actual_departure", 
            y="delay_in_min", 
            color="train_type",
            title="Each point represents one delayed train",
            labels={"actual_departure": "Departure time", "delay_in_min": "Delay (min)"},
            hover_data=["train_id", "origin", "destination"]
        )
        
        # Time lines - convert to timestamp
        now = pd.Timestamp.now()
        midnight = now.normalize()  # Midnight of current day
        
        fig_scatter.add_vline(x=now.value, line_color="red", line_dash="solid", annotation_text="Now")
        
        if df_analytics['actual_departure'].min() < midnight:
            fig_scatter.add_vline(x=midnight.value, line_color="gray", line_dash="dash", annotation_text="00:00")
        
        st.plotly_chart(fig_scatter, use_container_width=True)

        # --- 3. Statistical charts ---
        st.subheader("📉 Distribution Analysis")
        col_hist, col_box = st.columns(2)

        with col_hist:
            fig_hist = px.histogram(
                df_analytics, 
                x="delay_in_min", 
                nbins=30,
                title="Delay Histogram",
                labels={"delay_in_min": "Delay (minutes)"},
                color_discrete_sequence=['#EF553B']
            )
            fig_hist.update_layout(yaxis_title="Number of trains")
            st.plotly_chart(fig_hist, use_container_width=True)

        with col_box:
            fig_box = px.box(
                df_analytics, 
                x="train_type", 
                y="delay_in_min", 
                color="train_type",
                title="Delay Boxplot by Type",
                labels={"train_type": "Type", "delay_in_min": "Delay (min)"}
            )
            st.plotly_chart(fig_box, use_container_width=True)

        # --- 4. Detailed table ---
        with st.expander("🔎 Detailed Data (last 50 records)"):
            detailed_query = f"""
                SELECT 
                    train_id, 
                    origin,
                    destination,
                    planned_departure, 
                    actual_departure, 
                    delay_in_min,
                    is_cancelled
                FROM train_delays
                WHERE city = '{city}'
                  AND delay_in_min > 0
                  AND train_type IN {tuple(selected_types) if len(selected_types) > 1 else f"('{selected_types[0]}')"}
                ORDER BY actual_departure DESC
                LIMIT 50
            """
            st.dataframe(client.query_df(detailed_query))
            
    else:
        st.info(f"No data for {city} in the last 24 hours.")

except Exception as e:
    st.error(f"Application error: {e}")
    import traceback
    st.code(traceback.format_exc())