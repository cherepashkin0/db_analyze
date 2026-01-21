import asyncio
import os
import clickhouse_connect
from api_client import fetch_and_save # Твой скрипт выше
from iris_parser import parse_db_xml
import logging

# EVA IDs для Берлина, Кельна и Мюнхена
STATIONS = {
    "8011160": "Berlin",
    "8000207": "Köln",
    "8000261": "München"
}

async def run_real_ingestion():
    # 1. Получаем пароль
    ch_pass = os.getenv('CLICKHOUSE_PASSWORD')
    ch_user = os.getenv('CLICKHOUSE_USER', 'default')
    ch_host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
    
    # ЛОГИРУЕМ ДЛЯ ПРОВЕРКИ (только в целях отладки!)
    logging.info(f"Connecting to {ch_host} as {ch_user}. Password length: {len(ch_pass) if ch_pass else 0}")

    if not ch_pass:
        logging.error("CRITICAL: CLICKHOUSE_PASSWORD is empty or None!")

    client = clickhouse_connect.get_client(
        host=ch_host,
        username=ch_user,
        password=ch_pass
    )

    # 1. Готовим запросы для API (FCHG - Full Changes)
    queries = [
        {"url": f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{eva}"}
        for eva in STATIONS.keys()
    ]
    
    # 2. Вытягиваем данные и сохраняем в Parquet (Bronze Layer)
    output_path = "/opt/airflow/data/raw_timetables"
    df = await fetch_and_save(queries, output_path)
    
    # 3. Парсим XML и готовим данные для ClickHouse (Silver Layer)
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        username='default',
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )
    
    all_records = []
    for _, row in df.iterrows():
        if row['response_data']:
            # Определяем город по URL (вытаскиваем EVA ID из конца ссылки)
            eva_id = row['url'].split('/')[-1]
            city = STATIONS.get(eva_id, "Unknown")
            
            parsed_rows = parse_db_xml(row['response_data'])
            for r in parsed_rows:
                r['city'] = city
                all_records.append([r['timestamp'], r['city'], r['train_type'], r['delay_in_min']])

    # 4. Вставка в ClickHouse
    if all_records:
        client.insert('train_delays', all_records, 
                      column_names=['timestamp', 'city', 'train_type', 'delay_in_min'])
        print(f"✅ Успешно загружено {len(all_records)} реальных записей.")

def main():
    asyncio.run(run_real_ingestion())

if __name__ == "__main__":
    main()