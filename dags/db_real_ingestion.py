# db_real_ingestion.py

import asyncio
import os
import json
import clickhouse_connect
from datetime import datetime, timedelta
from api_client import fetch_and_save
from iris_parser import parse_db_xml
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- ФУНКЦИЯ ЗАГРУЗКИ КОНФИГА ---
def load_config():
    """Загружает конфигурацию станций и типов поездов из JSON файла."""
    base_dir = "/opt/airflow/dags"
    config_path = os.path.join(base_dir, "config", "railway_config.json")
    
    print(f"🔍 Ищу конфиг здесь: {config_path}")

    # === ОТЛАДКА (DEBUG) ===
    try:
        config_dir = os.path.join(base_dir, "config")
        if os.path.exists(config_dir):
            print(f"📂 Содержимое папки {config_dir}: {os.listdir(config_dir)}")
        else:
            print(f"❌ Папка {config_dir} не существует!")
            print(f"📂 Содержимое корня {base_dir}: {os.listdir(base_dir)}")
    except Exception as e:
        print(f"⚠ Ошибка при отладке путей: {e}")
    # =======================

    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                print(f"✅ Конфиг успешно загружен: {len(config.get('stations', {}))} станций")
                return config
        except Exception as e:
            print(f"❌ Файл есть, но ошибка чтения JSON: {e}")
    else:
        print("❌ Файл конфига физически отсутствует по этому пути.")

    # Дефолтный конфиг
    print("⚠ Использую дефолтные значения (12 основных станций).")
    return {
        "stations": {
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
        },
        "monitored_types": ["ICE", "IC", "EC", "ECE", "RE", "RB", "S", "NX", "FLX", "NJ"],
        "hours_to_fetch": 6  # Количество часов для загрузки (включая текущий)
    }


# --- ЛОГИРОВАНИЕ В POSTGRES ---
def log_ingestion_status(context, status, records_count, stations_count=0, error_message=None):
    """Записывает статус выполнения загрузки в Postgres."""
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        create_sql = """
        CREATE TABLE IF NOT EXISTS api_ingestion_log (
            run_id SERIAL PRIMARY KEY,
            dag_id VARCHAR(50),
            execution_date TIMESTAMP,
            status VARCHAR(20),
            records_count INT,
            stations_count INT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        pg_hook.run(create_sql)

        insert_sql = """
            INSERT INTO api_ingestion_log 
            (dag_id, execution_date, status, records_count, stations_count, error_message)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        
        dag_id = str(context['dag'].dag_id)
        execution_date = str(context.get('execution_date', datetime.now()))
        
        pg_hook.run(insert_sql, parameters=(
            dag_id, execution_date, status, records_count, stations_count, error_message
        ))
        print(f"📝 Статус '{status}' записан в Postgres (записей: {records_count}).")
    except Exception as e:
        print(f"❌ Ошибка записи лога в Postgres: {e}")


def build_plan_queries(stations: dict, hours_to_fetch: int = 6) -> list:
    """
    Формирует список запросов к /plan/{evaNo}/{date}/{hour} endpoint.
    
    Args:
        stations: Словарь {eva_id: station_name}
        hours_to_fetch: Количество часов для загрузки (начиная с текущего)
    
    Returns:
        Список словарей с URL и параметрами для каждого запроса
    """
    queries = []
    now = datetime.now()
    
    for eva_id, station_name in stations.items():
        for hour_offset in range(hours_to_fetch):
            # Вычисляем целевое время
            target_time = now + timedelta(hours=hour_offset)
            
            # Формат даты: YYMMDD
            date_str = target_time.strftime('%y%m%d')
            # Формат часа: HH (с ведущим нулём)
            hour_str = target_time.strftime('%H')
            
            queries.append({
                "url": f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/{eva_id}/{date_str}/{hour_str}",
                "params": {},
                # Метаданные для удобства обработки результатов
                "_meta": {
                    "eva_id": eva_id,
                    "station_name": station_name,
                    "date": date_str,
                    "hour": hour_str,
                    "type": "plan"
                }
            })
    
    return queries


def build_fchg_queries(stations: dict) -> list:
    """
    Формирует список запросов к /fchg/{evaNo} endpoint для получения 
    актуальных изменений (задержки, отмены, изменения платформ).
    
    Args:
        stations: Словарь {eva_id: station_name}
    
    Returns:
        Список словарей с URL и параметрами
    """
    queries = []
    
    for eva_id, station_name in stations.items():
        queries.append({
            "url": f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{eva_id}",
            "params": {},
            "_meta": {
                "eva_id": eva_id,
                "station_name": station_name,
                "type": "fchg"
            }
        })
    
    return queries


# --- ОСНОВНАЯ ЛОГИКА ---
async def run_real_ingestion(context):
    """
    Основная функция загрузки данных:
    1. Загружает конфиг со станциями
    2. Формирует запросы к API:
       - /plan/{evaNo}/{date}/{hour} — плановое расписание за несколько часов
       - /fchg/{evaNo} — актуальные изменения (опционально)
    3. Получает данные из API
    4. Парсит XML и загружает в ClickHouse
    """
    config = load_config()
    stations = config.get("stations", {})
    hours_to_fetch = config.get("hours_to_fetch", 6)
    fetch_realtime_changes = config.get("fetch_realtime_changes", True)
    
    # Убираем пустые типы и приводим к set
    target_types = set(filter(None, config.get("monitored_types", [])))
    
    if not stations:
        error_msg = "CRITICAL: No stations configured!"
        log_ingestion_status(context, 'FAILED', 0, 0, error_msg)
        raise Exception(error_msg)
    
    # === ФОРМИРОВАНИЕ ЗАПРОСОВ К API ===
    print(f"\n{'='*60}")
    print(f"🚂 DEUTSCHE BAHN TIMETABLE INGESTION")
    print(f"{'='*60}")
    print(f"📍 Станций: {len(stations)}")
    print(f"⏰ Часов для загрузки: {hours_to_fetch}")
    print(f"🔄 Загрузка real-time изменений: {'Да' if fetch_realtime_changes else 'Нет'}")
    if target_types:
        print(f"🎯 Фильтр типов поездов: {', '.join(sorted(target_types))}")
    else:
        print(f"🎯 Фильтрация типов: отключена (все поезда)")
    print(f"{'='*60}\n")
    
    # Формируем запросы к /plan/ endpoint
    plan_queries = build_plan_queries(stations, hours_to_fetch)
    print(f"📋 Сформировано {len(plan_queries)} запросов к /plan/ endpoint")
    print(f"   ({len(stations)} станций × {hours_to_fetch} часов)")
    
    # Опционально добавляем запросы к /fchg/ для real-time данных
    fchg_queries = []
    if fetch_realtime_changes:
        fchg_queries = build_fchg_queries(stations)
        print(f"📋 Сформировано {len(fchg_queries)} запросов к /fchg/ endpoint")
    
    all_queries = plan_queries + fchg_queries
    print(f"📊 Всего запросов к API: {len(all_queries)}")
    
    output_path = "/opt/airflow/data/raw_api_data"
    
    # === ЗАГРУЗКА ДАННЫХ ИЗ API ===
    print(f"\n🌐 Начинаю загрузку данных из API...")
    
    df = await fetch_and_save(
        queries=all_queries,
        output_path=output_path,
        max_concurrent=5,   # Ограничиваем concurrency для стабильности
        rate_limit=60       # DB API limit: 60 запросов/минуту
    )

    # === ПРОВЕРКА НА ОШИБКИ ===
    failed_requests = df['error'].notna().sum()
    total_requests = len(all_queries)
    successful_requests = total_requests - failed_requests
    
    print(f"\n📊 Статистика API запросов:")
    print(f"   ✅ Успешных: {successful_requests}")
    print(f"   ❌ Неудачных: {failed_requests}")
    print(f"   📈 Success rate: {successful_requests/total_requests*100:.1f}%")

    # Если все запросы упали - критическая ошибка
    if failed_requests == total_requests and total_requests > 0:
        error_msg = f"CRITICAL: All {total_requests} API requests failed."
        log_ingestion_status(context, 'FAILED', 0, len(stations), error_msg)
        raise Exception(error_msg)

    # Если часть запросов упала - логируем детали
    if failed_requests > 0:
        print(f"\n⚠ Детали неудачных запросов:")
        for _, row in df[df['error'].notna()].iterrows():
            print(f"   - {row['url']}: {row['error']}")

    # === ПОДКЛЮЧЕНИЕ К CLICKHOUSE ===
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        port=8123,
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )

    # === ПАРСИНГ И ФИЛЬТРАЦИЯ ДАННЫХ ===
    print(f"\n🔄 Парсинг XML ответов...")
    
    all_parsed_data = []
    stations_with_data = set()
    plan_records = 0
    fchg_records = 0
    
    for _, row in df.iterrows():
        # Пропускаем упавшие запросы
        if row['error']:
            continue

        # Извлекаем метаданные из URL
        url_parts = row['url'].split('/')
        
        # Определяем тип запроса и извлекаем EVA ID
        if '/plan/' in row['url']:
            # URL: .../plan/{evaNo}/{date}/{hour}
            eva_id = url_parts[-3]
            request_type = "plan"
            date_hour = f"{url_parts[-2]}/{url_parts[-1]}"
        elif '/fchg/' in row['url']:
            # URL: .../fchg/{evaNo}
            eva_id = url_parts[-1]
            request_type = "fchg"
            date_hour = "realtime"
        else:
            print(f"  ⚠ Неизвестный формат URL: {row['url']}")
            continue
        
        city = stations.get(eva_id, "Unknown")
        
        if row['response_data']:
            # === ОТЛАДКА: показываем превью XML ===
            xml_length = len(row['response_data'])
            if xml_length > 0:
                xml_preview = row['response_data'][:300] if xml_length > 300 else row['response_data']
                # Убираем переносы строк для компактности
                xml_preview_clean = ' '.join(xml_preview.split())
                print(f"\n  📄 {city} [{request_type}] ({date_hour}):")
                print(f"     XML size: {xml_length} chars")
                print(f"     Preview: {xml_preview_clean[:150]}...")
            
            # Парсим XML ответ
            parsed_rows = parse_db_xml(row['response_data'], city)
            
            if parsed_rows:
                print(f"     Parsed: {len(parsed_rows)} записей")
                
                # Применяем фильтрацию по типам поездов
                if target_types:
                    before_filter = len(parsed_rows)
                    parsed_rows = [r for r in parsed_rows if r[2] in target_types]
                    after_filter = len(parsed_rows)
                    if before_filter != after_filter:
                        print(f"     Filtered: {before_filter} → {after_filter} записей")
                
                if parsed_rows:
                    all_parsed_data.extend(parsed_rows)
                    stations_with_data.add(city)
                    
                    if request_type == "plan":
                        plan_records += len(parsed_rows)
                    else:
                        fchg_records += len(parsed_rows)
            else:
                print(f"     ⚠ Парсер не вернул данных")

    total_records = len(all_parsed_data)
    
    print(f"\n{'='*60}")
    print(f"📈 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"   Станций запрошено: {len(stations)}")
    print(f"   Станций с данными: {len(stations_with_data)}")
    print(f"   Записей из /plan/: {plan_records}")
    print(f"   Записей из /fchg/: {fchg_records}")
    print(f"   ВСЕГО записей: {total_records}")
    if target_types:
        print(f"   Типы поездов: {', '.join(sorted(target_types))}")
    print(f"{'='*60}\n")

    # === ЗАГРУЗКА В CLICKHOUSE ===
    if all_parsed_data:
        try:
            client.insert(
                'train_delays', 
                all_parsed_data, 
                column_names=[
                    'timestamp', 'city', 'train_type', 'train_id', 
                    'planned_departure', 'actual_departure', 
                    'delay_in_min', 'is_cancelled',
                    'origin', 'destination'
                ]
            )
            print(f"✅ Успешно загружено {total_records} строк в ClickHouse.")
            log_ingestion_status(context, 'SUCCESS', total_records, len(stations_with_data))
        except Exception as e:
            error_msg = f"Failed to insert into ClickHouse: {str(e)}"
            print(f"❌ {error_msg}")
            log_ingestion_status(context, 'FAILED', 0, len(stations_with_data), error_msg)
            raise
    else:
        warning_msg = "API ответил успешно, но данных нет (или все отфильтрованы)."
        print(f"⚠ {warning_msg}")
        log_ingestion_status(context, 'WARNING', 0, len(stations_with_data), warning_msg)


def main(**kwargs):
    """Entry point для Airflow DAG."""
    asyncio.run(run_real_ingestion(kwargs))