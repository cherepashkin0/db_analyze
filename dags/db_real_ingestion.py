import asyncio
import os
import clickhouse_connect
from api_client import fetch_and_save
from iris_parser import parse_db_xml

# Справочник EVA ID для запросов
STATIONS = {
    "8011160": "Berlin",
    "8000207": "Köln",
    "8000261": "München"
}

async def run_real_ingestion():
    # 1. Формируем список URL для API (Changes API)
    queries = [
        {"url": f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{eva}"}
        for eva in STATIONS.keys()
    ]
    
    # 2. Асинхронно скачиваем данные (Bronze Layer - сохраняем в Parquet)
    # Путь внутри контейнера Airflow
    output_path = "/opt/airflow/data/raw_api_data"
    
    df = await fetch_and_save(
        queries=queries,
        output_path=output_path,
        max_concurrent=3, # Не будем частить, чтобы не забанили
        rate_limit=60
    )

    # 3. Трансформация (Silver Layer - парсим и готовим для ClickHouse)
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )

    all_parsed_data = []
    
    for _, row in df.iterrows():
        eva_id = row['url'].split('/')[-1]
        city = STATIONS.get(eva_id, "Unknown")
        
        if row['response_data']:
            parsed_rows = parse_db_xml(row['response_data'], city)
            all_parsed_data.extend(parsed_rows)

    # 4. Вставка в ClickHouse
    if all_parsed_data:
        client.insert('train_delays', all_parsed_data, 
                      column_names=['timestamp', 'city', 'train_type', 'delay_in_min'])
        print(f"✅ Успешно загружено {len(all_parsed_data)} РЕАЛЬНЫХ строк из API DB.")
    else:
        print("⚠ API ответил, но новых задержек в этот раз не найдено.")

def main():
    asyncio.run(run_real_ingestion())