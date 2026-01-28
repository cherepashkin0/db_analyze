import streamlit as st
import clickhouse_connect
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import math 
import numpy as np

# 1. Config
st.set_page_config(page_title="DB Punctuality Tracker", layout="wide")

CH_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASS = os.getenv('CLICKHOUSE_PASSWORD')

@st.cache_resource
def get_clickhouse_client():
    return clickhouse_connect.get_client(host=CH_HOST, port=8123, username=CH_USER, password=CH_PASS)

client = get_clickhouse_client()

# --- Helper Functions ---
def get_filter_data():
    """Получаем списки городов и типов сразу одним легким запросом."""
    try:
        cities = client.query("SELECT DISTINCT city FROM train_delays FINAL ORDER BY city").result_rows
        types = client.query("SELECT DISTINCT train_type FROM train_delays FINAL ORDER BY train_type").result_rows
        return [c[0] for c in cities], [t[0] for t in types]
    except:
        return [], []

# --- UI Layout ---
st.title("🚆 DB Punctuality Tracker")

available_cities, available_types = get_filter_data()

# Sidebar
st.sidebar.header("Filters")
city = st.sidebar.selectbox("Select City", available_cities if available_cities else ["Berlin Hbf"])
selected_types = st.sidebar.multiselect("Train Types", available_types, default=available_types)

if not selected_types:
    st.stop()

types_tuple = tuple(selected_types) if len(selected_types) > 1 else f"('{selected_types[0]}')"

# --- 1. KPI BLOCK (Using GOLD Table) ---
st.subheader("📈 Daily Stats")

# 1. Глобальные метрики (Сумма по всем выбранным типам)
gold_global_query = f"""
SELECT 
    sum(total_trains),
    sum(delayed_trains),
    avgIf(avg_delay, avg_delay > 0), -- Среднее только среди опоздавших
    max(max_delay)
FROM daily_train_stats FINAL
WHERE city = '{city}' 
  AND stat_date = toDate(now())
  AND train_type IN {types_tuple}
"""

# 2. Детальная статистика по типам (GROUP BY train_type)
gold_by_type_query = f"""
SELECT 
    train_type,
    sum(total_trains) as total,
    sum(delayed_trains) as delayed,
    avgIf(avg_delay, avg_delay > 0) as avg_delay,
    max(max_delay) as max_delay
FROM daily_train_stats FINAL
WHERE city = '{city}' 
  AND stat_date = toDate(now())
  AND train_type IN {types_tuple}
GROUP BY train_type
ORDER BY total DESC
"""

try:
    # --- A. Отображение глобальных карточек ---
    result_global = client.query(gold_global_query).result_rows
    
    if result_global and result_global[0][0] is not None:
        total, delayed, avg_del, max_del = result_global[0]
        
        # Очистка данных
        total = total or 0
        delayed = delayed or 0
        max_del = max_del or 0
        avg_del = avg_del if (avg_del is not None and not math.isnan(avg_del)) else 0.0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Trains", total)
        
        delay_rate = (delayed / total * 100) if total > 0 else 0
        k2.metric("Delayed Trains", delayed, delta=f"{delay_rate:.1f}% rate", delta_color="inverse")
        
        k3.metric("Avg Delay (Delayed)", f"{avg_del:.1f} min")
        k4.metric("Max Delay", f"{max_del} min")

    else:
        st.warning(f"No aggregated stats found for {city} today.")

    st.divider()

    # --- B. Таблица статистики по типам поездов ---
    st.subheader("📋 Statistics by Train Type")
    
    df_by_type = client.query_df(gold_by_type_query)
    
    if not df_by_type.empty:
        # Добавляем колонку с процентом опозданий для красоты
        df_by_type['delay_rate'] = (df_by_type['delayed'] / df_by_type['total'] * 100).fillna(0)
        
        # Переименуем колонки для красивого отображения
        st.dataframe(
            df_by_type,
            use_container_width=True,
            column_order=["train_type", "total", "delayed", "delay_rate", "avg_delay", "max_delay"],
            column_config={
                "train_type": "Type",
                "total": st.column_config.NumberColumn("Total Trains", format="%d"),
                "delayed": st.column_config.NumberColumn("Delayed", format="%d"),
                "delay_rate": st.column_config.ProgressColumn(
                    "Delay Rate", 
                    format="%.1f%%", 
                    min_value=0, 
                    max_value=100,
                    help="Percentage of trains delayed > 5 min"
                ),
                "avg_delay": st.column_config.NumberColumn("Avg Delay (min)", format="%.1f"),
                "max_delay": st.column_config.NumberColumn("Max Delay (min)", format="%d"),
            },
            hide_index=True
        )
    else:
        st.info("No data available for breakdown by type.")

except Exception as e:
    st.error(f"Error loading Gold layer: {e}")

# Debug: показать сырые данные из Gold layer
with st.expander("🔍 Debug: Raw Gold Layer Data"):
    debug_query = f"""
    SELECT 
        stat_date,
        city,
        train_type,
        total_trains,
        delayed_trains,
        avg_delay,
        max_delay,
        created_at
    FROM daily_train_stats FINAL
    WHERE city = '{city}' 
      AND stat_date = toDate(now())
    ORDER BY train_type
    """
    try:
        debug_df = client.query_df(debug_query)
        if not debug_df.empty:
            st.dataframe(debug_df, use_container_width=True)
        else:
            st.write("No data in Gold layer for today. Make sure Airflow DAG ran successfully.")
    except Exception as e:
        st.write(f"Debug query error: {e}")

st.divider()

# --- 2. DRILL DOWN CHARTS (Using RAW Table) ---
st.subheader(f"📊 Planned vs Actual Departures — {city}")

raw_query = f"""
SELECT
    planned_departure,
    actual_departure,
    train_type,
    delay_in_min,
    train_id,
    origin,
    destination,
    is_cancelled
FROM train_delays FINAL
WHERE city = '{city}'
  AND planned_departure >= now() - INTERVAL 24 HOUR
  AND planned_departure <= now() + INTERVAL 12 HOUR
  AND train_type IN {types_tuple}
ORDER BY planned_departure ASC
"""

df_raw = client.query_df(raw_query)

if not df_raw.empty:
    # Конвертация дат
    df_raw['planned_departure'] = pd.to_datetime(df_raw['planned_departure'])
    df_raw['actual_departure'] = pd.to_datetime(df_raw['actual_departure'])
    
    # Добавляем статус для цвета (стандарт DB: опоздание > 5 мин)
    def get_status(row):
        if row['is_cancelled'] == 1:
            return 'Cancelled'
        elif row['delay_in_min'] > 5:
            return 'Delayed'
        elif row['delay_in_min'] < -1:
            return 'Early'
        else:
            return 'On Time'
    
    df_raw['status'] = df_raw.apply(get_status, axis=1)
    
    # --- График: Planned vs Actual ---
    fig = go.Figure()
    
    # Цвета для статусов
    color_map = {
        'On Time': '#2ecc71',
        'Delayed': '#e74c3c', 
        'Early': '#3498db',
        'Cancelled': '#95a5a6'
    }
    
    for status in ['On Time', 'Early', 'Delayed', 'Cancelled']:
        df_status = df_raw[df_raw['status'] == status]
        if not df_status.empty:
            fig.add_trace(go.Scatter(
                x=df_status['planned_departure'],
                y=df_status['actual_departure'],
                mode='markers',
                name=status,
                marker=dict(color=color_map[status], size=8, opacity=0.7),
                hovertemplate=(
                    '<b>%{customdata[0]}</b><br>' +
                    'Planned: %{x}<br>' +
                    'Actual: %{y}<br>' +
                    'Delay: %{customdata[1]} min<br>' +
                    'Route: %{customdata[2]} → %{customdata[3]}<extra></extra>'
                ),
                customdata=df_status[['train_id', 'delay_in_min', 'origin', 'destination']].values
            ))
    
    # Диагональная линия "идеальное расписание" (planned = actual)
    min_time = df_raw['planned_departure'].min()
    max_time = df_raw['planned_departure'].max()
    fig.add_trace(go.Scatter(
        x=[min_time, max_time],
        y=[min_time, max_time],
        mode='lines',
        name='Perfect (no delay)',
        line=dict(color='gray', dash='dash', width=1)
    ))
    
    # Линия "сейчас"
    now = datetime.now()
    fig.add_shape(
        type="line",
        x0=now, x1=now,
        y0=0, y1=1,
        yref="paper",
        line=dict(color="red", dash="dot", width=2)
    )
    fig.add_annotation(x=now, y=1, yref="paper", text="Now", showarrow=False, yanchor="bottom")
    
    fig.update_layout(
        title="Planned vs Actual Departure Time",
        xaxis_title="Planned Departure",
        yaxis_title="Actual Departure",
        height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # --- Второй график: Delays Timeline ---
    st.subheader("⏱ Delays Timeline")
    
    df_delayed = df_raw[df_raw['delay_in_min'] != 0]
    
    if not df_delayed.empty:
        fig2 = px.scatter(
            df_delayed,
            x="planned_departure",
            y="delay_in_min",
            color="train_type",
            title="Delay Distribution Over Time",
            hover_data=["train_id", "origin", "destination"],
            labels={"delay_in_min": "Delay (min)", "planned_departure": "Planned Departure"}
        )
        
        fig2.add_hline(y=0, line_dash="dash", line_color="green", annotation_text="On time")
        now2 = datetime.now()
        fig2.add_shape(
            type="line",
            x0=now2, x1=now2,
            y0=0, y1=1,
            yref="paper",
            line=dict(color="red", dash="dot", width=2)
        )
        fig2.add_annotation(x=now2, y=1, yref="paper", text="Now", showarrow=False, yanchor="bottom")
        
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)
    
    # --- Таблица ---
    with st.expander("📋 Detailed Train Log"):
        display_df = df_raw[[
            'train_id', 'train_type', 'planned_departure', 'actual_departure', 
            'delay_in_min', 'status', 'origin', 'destination'
        ]].sort_values(by='planned_departure', ascending=False)
        
        st.dataframe(
            display_df.head(100),
            use_container_width=True,
            column_config={
                "train_id": "Train",
                "train_type": "Type",
                "planned_departure": st.column_config.DatetimeColumn("Planned", format="DD.MM.YYYY HH:mm"),
                "actual_departure": st.column_config.DatetimeColumn("Actual", format="DD.MM.YYYY HH:mm"),
                "delay_in_min": st.column_config.NumberColumn("Delay", format="%d min"),
                "status": "Status",
                "origin": "From",
                "destination": "To"
            }
        )

    st.divider()
    
# --- 3. HISTOGRAM: Delay Distribution ---
    st.subheader("📊 Delay Distribution")
    
    fig_hist = px.histogram(
        df_raw,
        x="delay_in_min",
        nbins=50,
        color="status",
        color_discrete_map={
            'On Time': '#2ecc71',
            'Delayed': '#e74c3c',
            'Early': '#3498db',
            'Cancelled': '#95a5a6'
        },
        title="Distribution of Delays (minutes)",
        labels={"delay_in_min": "Delay (min)", "count": "Number of Trains"}
    )
    
    # --- ЛОГИКА МАСШТАБИРОВАНИЯ ---
    # Мы берем подмножество только реально опоздавших (например, > 4 мин),
    # чтобы найти "пик" среди них и подстроить график под этот пик.
    delayed_subset = df_raw[df_raw['delay_in_min'] >= 4]
    
    if not delayed_subset.empty:
        # Эмулируем гистограмму, чтобы узнать высоту самого высокого "опоздавшего" столбика
        counts, _ = np.histogram(delayed_subset['delay_in_min'], bins=50)
        max_delayed_count = counts.max()
        
        # Устанавливаем лимит оси Y: высота опоздавших + 20% запаса
        # Столбик "On Time" (0 мин) будет обрезан сверху.
        fig_hist.update_yaxes(range=[0, max_delayed_count * 1.2])
    
    # Добавляем линии порогов
    fig_hist.add_vline(x=0, line_dash="dash", line_color="green")
    fig_hist.add_vline(x=5, line_dash="dot", line_color="orange", annotation_text="5 min threshold")
    
    fig_hist.update_layout(height=400, bargap=0.1)
    st.plotly_chart(fig_hist, use_container_width=True)
    
    # --- 4. BOXPLOT: Delays by Train Type ---
    st.subheader("📦 Delays by Train Type")
    
    fig_box = px.box(
        df_raw,
        x="train_type",
        y="delay_in_min",
        color="train_type",
        title="Delay Distribution by Train Type",
        labels={"delay_in_min": "Delay (min)", "train_type": "Train Type"},
        points="outliers"  # показываем только выбросы, не все точки
    )
    fig_box.add_hline(y=0, line_dash="dash", line_color="green", annotation_text="On time")
    fig_box.add_hline(y=5, line_dash="dot", line_color="orange", annotation_text="5 min threshold")
    fig_box.update_layout(height=500, showlegend=False)
    st.plotly_chart(fig_box, use_container_width=True)
    
    # --- Summary stats table ---
    with st.expander("📈 Summary Statistics by Train Type"):
        summary = df_raw.groupby('train_type').agg({
            'delay_in_min': ['count', 'mean', 'median', 'std', 'min', 'max'],
            'is_cancelled': 'sum'
        }).round(1)
        summary.columns = ['Total', 'Avg Delay', 'Median Delay', 'Std Dev', 'Min', 'Max', 'Cancelled']
        summary = summary.sort_values('Total', ascending=False)
        st.dataframe(summary, use_container_width=True)
        
else:
    st.info("No data available for the selected filters.")