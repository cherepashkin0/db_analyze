# db_real_ingestion.py

import asyncio
import os
import clickhouse_connect
from api_client import fetch_and_save
from iris_parser import parse_db_xml
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils.email import send_email

# Справочник EVA ID
STATIONS = {
    "8011160": "Berlin Hbf",
    "8000207": "Köln Hbf",
    "8000261": "München Hbf",
    "8000105": "Frankfurt (Main) Hbf",
    "8002549": "Hamburg Hbf",
    "8000096": "Stuttgart Hbf",
    "8000244": "Mannheim Hbf",
    "8000191": "Karlsruhe Hbf",
    "8000284": "Nürnberg Hbf",
    "8000152": "Hannover Hbf",
    "8000080": "Dortmund Hbf",
    "8000260": "Würzburg Hbf"
}

# --- УНИВЕРСАЛЬНАЯ ФУНКЦИЯ ЛОГИРОВАНИЯ ---
def log_ingestion_status(context, status, records_count, error_message=None):
    """Пишет статус (SUCCESS/FAILED) в Postgres"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Если таблицы еще нет, создаем (на всякий случай)
    create_sql = """
    CREATE TABLE IF NOT EXISTS api_ingestion_log (
        run_id SERIAL PRIMARY KEY,
        dag_id VARCHAR(50),
        execution_date VARCHAR(50),
        status VARCHAR(20),
        records_count INT,
        error_message TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    pg_hook.run(create_sql)

    insert_sql = """
        INSERT INTO api_ingestion_log (dag_id, execution_date, status, records_count, error_message)
        VALUES (%s, %s, %s, %s, %s);
    """
    
    dag_id = str(context['dag'].dag_id)
    execution_date = str(context['execution_date'])
    
    pg_hook.run(insert_sql, parameters=(dag_id, execution_date, status, records_count, error_message))
    print(f"📝 Статус '{status}' записан в Postgres.")

# -------------------------------------------

async def run_real_ingestion(context):
    queries = [
        {"url": f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{eva}"}
        for eva in STATIONS.keys()
    ]
    
    output_path = "/opt/airflow/data/raw_api_data"
    
    df = await fetch_and_save(
        queries=queries,
        output_path=output_path,
        max_concurrent=3,
        rate_limit=60
    )

    # === БЛОК ПРОВЕРКИ НА ОШИБКИ ===
    # Считаем, сколько запросов вернули ошибку (колонка error не пустая)
    failed_requests = df['error'].notna().sum()
    total_requests = len(queries)
    
    print(f"📊 Статистика: {total_requests - failed_requests}/{total_requests} успешных запросов.")

    # Если 100% запросов упали - это критическая ошибка
    if failed_requests == total_requests:
        error_msg = f"CRITICAL: All {total_requests} API requests failed. Check logs for details."
        
        # 1. Пишем FAIL в Postgres
        log_ingestion_status(context, 'FAILED', 0, error_msg)
        
        # 2. БРОСАЕМ ИСКЛЮЧЕНИЕ -> Это вызовет on_failure_callback (Telegram)
        raise Exception(error_msg)
    # ===============================

    # 2. Если мы здесь, значит хоть какие-то данные есть
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )

    all_parsed_data = []
    
    for _, row in df.iterrows():
        # Пропускаем строки с ошибками
        if row['error']:
            continue

        eva_id = row['url'].split('/')[-1]
        city = STATIONS.get(eva_id, "Unknown")
        
        if row['response_data']:
            parsed_rows = parse_db_xml(row['response_data'], city)
            all_parsed_data.extend(parsed_rows)

    count = len(all_parsed_data)

    if all_parsed_data:
        client.insert('train_delays', all_parsed_data, 
                        column_names=[
                            'timestamp', 'city', 'train_type', 'train_id', 
                            'planned_departure', 'actual_departure', 
                            'delay_in_min', 'is_cancelled'
                        ])
        print(f"✅ Успешно загружено {count} строк.")
    else:
        print("⚠ API доступен, но данных о задержках нет.")
        # 3. Пишем SUCCESS в Postgres
        log_ingestion_status(context, 'SUCCESS', count)

    # log_success_to_postgres(context, count)


# def log_success_to_postgres(context, records_count):
#     """Пишет лог успешной загрузки в Postgres"""
#     pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
#     sql = """
#         INSERT INTO api_ingestion_log (dag_id, execution_date, status, records_count)
#         VALUES (%s, %s, 'SUCCESS', %s);
#     """
    
#     # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
#     # Мы принудительно превращаем объекты Airflow в строки.
#     # Это снимает обертку 'Proxy' и драйвер базы данных получает обычный текст.
    
#     dag_id = str(context['dag'].dag_id)
#     execution_date = str(context['execution_date']) # Превратит дату в ISO-строку
    
#     # -------------------------
    
#     pg_hook.run(sql, parameters=(dag_id, execution_date, records_count))
#     print(f"✅ Запись об успехе сохранена в Postgres (ID: {dag_id}).")


def main(**kwargs):
    asyncio.run(run_real_ingestion(kwargs))