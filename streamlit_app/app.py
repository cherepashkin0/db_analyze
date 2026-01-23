import streamlit as st
import clickhouse_connect
import pandas as pd
import os
import plotly.express as px

# 1. Настройка страницы (должна быть первой командой)
st.set_page_config(page_title="DB Punctuality Tracker", layout="wide")

# 2. Получаем настройки
CH_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASS = os.getenv('CLICKHOUSE_PASSWORD')

# 3. Функция подключения
@st.cache_resource
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=8123, 
        username=CH_USER, 
        password=CH_PASS
    )

client = get_clickhouse_client()

# --- НОВАЯ ФУНКЦИЯ: Получение списка городов из базы ---
def get_available_cities():
    """Спрашивает у ClickHouse, какие города уже есть в таблице."""
    try:
        # DISTINCT выбирает уникальные значения, ORDER BY сортирует по алфавиту
        df = client.query_df("SELECT DISTINCT city FROM train_delays ORDER BY city")
        if not df.empty:
            return df['city'].tolist()
    except Exception as e:
        print(f"Ошибка получения городов: {e}")
    
    # Фолбэк (на случай, если база пустая или недоступна)
    return ["Berlin", "Köln", "München"] 
# -------------------------------------------------------

st.title("🚆 DB Punctuality Index")
st.write("Данные загружаются напрямую из ClickHouse.")

# --- Боковая панель ---
st.sidebar.header("Фильтры")

# Используем динамический список
available_cities = get_available_cities()
city = st.sidebar.selectbox("Выберите город", available_cities)

# 4. Основной аналитический запрос (с фильтром будущего)
query_analytics = f"""
SELECT
    toStartOfInterval(actual_departure, INTERVAL 10 minute) as time_bucket,
    train_type,
    round(avg(delay_in_min), 1) as avg_delay
FROM train_delays
WHERE city = '{city}' 
  AND actual_departure >= now() - INTERVAL 24 HOUR  -- Данные за 24 часа
  AND actual_departure <= now()                     -- Фильтр будущего
  AND train_type IN ('ICE', 'IC', 'RE', 'RB', 'S')
GROUP BY time_bucket, train_type
ORDER BY time_bucket ASC
"""

st.subheader(f"📊 Динамика задержек в {city} (усреднение по 10 мин)")

try:
    df_analytics = client.query_df(query_analytics)

    if not df_analytics.empty:
        # Интерактивный график Plotly
        fig = px.line(df_analytics, x="time_bucket", y="avg_delay", color="train_type",
                      title="Средняя задержка по типам поездов",
                      labels={"time_bucket": "Время", "avg_delay": "Минут задержки"},
                      markers=True)
        st.plotly_chart(fig, use_container_width=True)
        
        # Детальная таблица с удалением дубликатов (LIMIT 1 BY)
        with st.expander("Детальные данные о поездах (последние 50)"):
            detailed_query = f"""
                SELECT 
                    train_id, 
                    planned_departure, 
                    actual_departure, 
                    delay_in_min,
                    is_cancelled  -- Добавим колонку отмены, раз она у нас есть
                FROM train_delays
                WHERE city = '{city}'
                  AND actual_departure <= now()
                ORDER BY actual_departure DESC
                LIMIT 1 BY train_id, planned_departure -- Убираем дубликаты
                LIMIT 50
            """
            st.dataframe(client.query_df(detailed_query))
            
    else:
        st.info(f"Данных по городу {city} за последние 24 часа пока нет. Airflow работает?")

except Exception as e:
    st.error(f"Ошибка запроса: {e}")