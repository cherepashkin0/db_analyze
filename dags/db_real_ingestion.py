import asyncio
import os
import json
import clickhouse_connect
from datetime import datetime, timedelta
from api_client import generate_plan_queries, fetch_and_save
from iris_parser import parse_plan_xml, parse_fchg_xml
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- КОНФИГУРАЦИЯ ---
def load_config():
    """Загружает конфигурацию станций."""
    base_dir = "/opt/airflow/dags"
    config_path = os.path.join(base_dir, "config", "railway_config.json")
    
    print(f"🔍 Ищу конфиг здесь: {config_path}")
    
    try:
        config_dir = os.path.join(base_dir, "config")
        if os.path.exists(config_dir):
            print(f"📂 Содержимое папки {config_dir}: {os.listdir(config_dir)}")
    except: pass

    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                print(f"✅ Конфиг успешно загружен: {len(config.get('stations', {}))} станций")
                return config
        except Exception as e:
            print(f"❌ Ошибка чтения JSON: {e}")
    
    return {
        "stations": {"8011160": "Berlin Hbf"}, 
        "monitored_types": []
    }

# --- HELPER: CLICKHOUSE CLIENT ---
def get_ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD')
    )

def ensure_clickhouse_tables():
    """Создаёт таблицы в ClickHouse если их нет."""
    client = get_ch_client()
    
    # Проверяем, нужна ли миграция train_delays на ReplacingMergeTree
    try:
        result = client.query("SELECT engine FROM system.tables WHERE name = 'train_delays' AND database = 'default'")
        if result.result_rows:
            current_engine = result.result_rows[0][0]
            if 'ReplacingMergeTree' not in current_engine:
                print(f"⚠ Миграция: train_delays использует {current_engine}, нужен ReplacingMergeTree")
                print("🔄 Пересоздаём таблицу train_delays...")
                client.command("DROP TABLE IF EXISTS train_delays")
    except Exception as e:
        print(f"⚠ Ошибка проверки движка: {e}")
    
    # Silver layer: train_delays (сырые данные)
    # ReplacingMergeTree автоматически дедуплицирует по ORDER BY ключу
    client.command("""
        CREATE TABLE IF NOT EXISTS train_delays (
            timestamp DateTime,
            city String,
            train_type String,
            train_id String,
            planned_departure DateTime,
            actual_departure DateTime,
            delay_in_min Int32,
            is_cancelled UInt8,
            origin String,
            destination String
        ) ENGINE = ReplacingMergeTree(timestamp)
        ORDER BY (city, train_id, planned_departure)
        PARTITION BY toYYYYMM(planned_departure)
    """)
    
    # Gold layer: daily_train_stats (агрегированные данные)
    client.command("""
        CREATE TABLE IF NOT EXISTS daily_train_stats (
            stat_date Date,
            city String,
            train_type String,
            total_trains UInt32,
            delayed_trains UInt32,
            avg_delay Float32,
            max_delay Int32,
            created_at DateTime
        ) ENGINE = ReplacingMergeTree(created_at)
        ORDER BY (stat_date, city, train_type)
        PARTITION BY toYYYYMM(stat_date)
    """)
    
    print("✅ ClickHouse tables ensured: train_delays, daily_train_stats")

# --- ЛОГИРОВАНИЕ ---
def log_status(context, stage, status, msg=""):
    """Пишет статус этапа в консоль и в Postgres."""
    print(f"[{stage}] {status}: {msg}")
    
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = """
            INSERT INTO api_ingestion_log (dag_id, execution_date, status, error_message)
            VALUES (%s, %s, %s, %s)
        """
        dag_id = str(context['dag'].dag_id)
        execution_date = str(context.get('execution_date', datetime.now()))
        
        pg_hook.run("""
            CREATE TABLE IF NOT EXISTS api_ingestion_log (
                run_id SERIAL PRIMARY KEY,
                dag_id VARCHAR(50),
                execution_date VARCHAR(50),
                status VARCHAR(20),
                error_message TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        pg_hook.run(sql, parameters=(dag_id, execution_date, status, f"{stage}: {msg}"))
    except Exception as e:
        print(f"⚠ Ошибка записи лога в Postgres: {e}")

# ==========================================
# 1. EXTRACT DATA (API -> Parquet/Bronze)
# ==========================================
async def extract_data(config):
    """
    Загружает данные из двух источников:
    1. /plan/{evaNo}/{YYMMDD}/{HH} - ВСЕ запланированные поезда
    2. /fchg/{evaNo} - актуальные изменения (задержки, отмены)
    
    Потом данные объединяются в load_to_silver.
    """
    from api_client import generate_plan_queries, generate_fchg_queries, fetch_and_save
    
    stations = config.get("stations", {})
    hours_back = config.get("hours_back", 24)
    hours_forward = config.get("hours_forward", 0)
    
    # 1. Генерируем запросы для плана (все поезда)
    plan_queries = generate_plan_queries(
        stations=stations,
        hours_back=hours_back,
        hours_forward=hours_forward,
    )
    
    # 2. Генерируем запросы для изменений (задержки)
    fchg_queries = generate_fchg_queries(stations=stations)
    
    # Объединяем все запросы
    all_queries = plan_queries + fchg_queries
    
    print(f"🌍 TASK 1: EXTRACT. Загрузка данных:")
    print(f"   - Станций: {len(stations)}")
    print(f"   - План запросов: {len(plan_queries)} (часы: -{hours_back} / +{hours_forward})")
    print(f"   - Изменения запросов: {len(fchg_queries)}")
    print(f"   - Всего запросов: {len(all_queries)}")
    
    return await fetch_and_save(
        queries=all_queries,
        output_path="/opt/airflow/data/raw_api_data",
        max_concurrent=10,
        rate_limit=60,
    )

# ==========================================
# 2. LOAD TO SILVER (Parquet -> ClickHouse Raw)
# ==========================================
def load_to_silver(df, config):
    """
    Парсит XML ответы и загружает в ClickHouse.
    
    Логика объединения:
    1. Парсим /plan данные -> базовое расписание (delay=0)
    2. Парсим /fchg данные -> изменения с реальными задержками
    3. Объединяем: fchg данные перезаписывают plan по ключу (train_id, planned_departure, city)
    """
    print("📥 TASK 2: LOAD TO SILVER...")
    
    target_types = set(config.get("monitored_types", []))
    
    # Словарь для хранения данных: ключ -> данные
    # Ключ: (train_id, planned_departure, city)
    trains_dict = {}
    
    plan_count = 0
    fchg_count = 0
    error_count = 0
    
    for _, row in df.iterrows():
        if row['error']:
            error_count += 1
            continue
            
        if not row['response_data']:
            continue
        
        station_name = row.get('station_name', 'Unknown')
        query_type = row.get('query_type', 'plan')
        
        try:
            # Выбираем парсер в зависимости от типа запроса
            if query_type == 'fchg':
                rows = parse_fchg_xml(row['response_data'], station_name)
                fchg_count += len(rows)
            else:
                rows = parse_plan_xml(row['response_data'], station_name)
                plan_count += len(rows)
            
            # Фильтруем по типу поезда если нужно
            if target_types:
                rows = [r for r in rows if r[2] in target_types]
            
            # Добавляем в словарь
            for row_data in rows:
                # row_data: (timestamp, city, train_type, train_id, planned_departure, 
                #            actual_departure, delay_in_min, is_cancelled, origin, destination)
                key = (row_data[3], row_data[4], row_data[1])  # (train_id, planned_departure, city)
                
                # fchg данные имеют приоритет (перезаписывают plan)
                if query_type == 'fchg':
                    trains_dict[key] = row_data
                elif key not in trains_dict:
                    # plan данные добавляем только если ещё нет записи
                    trains_dict[key] = row_data
            
        except Exception as e:
            print(f"⚠ Ошибка парсинга для {station_name}: {e}")
            error_count += 1
    
    print(f"📊 Парсинг завершён:")
    print(f"   - Plan записей: {plan_count}")
    print(f"   - Fchg записей (с изменениями): {fchg_count}")
    print(f"   - Ошибок: {error_count}")
    print(f"   - Уникальных поездов после объединения: {len(trains_dict)}")
    
    if not trains_dict:
        print("⚠ LOAD: Нет данных для вставки.")
        return 0

    # Преобразуем обратно в список
    all_parsed = list(trains_dict.values())
    
    # Статистика по задержкам
    delayed_count = sum(1 for r in all_parsed if r[6] > 0)
    cancelled_count = sum(1 for r in all_parsed if r[7] == 1)
    print(f"   - С задержкой: {delayed_count}")
    print(f"   - Отменено: {cancelled_count}")

    client = get_ch_client()
    
    client.insert('train_delays', all_parsed, 
                  column_names=['timestamp', 'city', 'train_type', 'train_id', 
                                'planned_departure', 'actual_departure', 
                                'delay_in_min', 'is_cancelled', 'origin', 'destination'])
    
    print(f"✅ LOAD: Вставлено {len(all_parsed)} строк в Silver слой (train_delays).")
    return len(all_parsed)

# ==========================================
# 3. DATA QUALITY CHECK (Validation)
# ==========================================
def data_quality_check():
    print("🧐 TASK 3: DATA QUALITY CHECK...")
    client = get_ch_client()
    
    # Критические проверки (роняют пайплайн)
    critical_checks = [
        ("Null Check: Train IDs", 
         "SELECT count() FROM train_delays FINAL WHERE train_id = '' AND timestamp > now() - INTERVAL 1 HOUR"),
         
        ("Null Check: Cities", 
         "SELECT count() FROM train_delays FINAL WHERE city = '' AND timestamp > now() - INTERVAL 1 HOUR"),

        ("Range Check: Negative Delays", 
         "SELECT count() FROM train_delays FINAL WHERE delay_in_min < -60"),
         
        ("Range Check: Extreme Delays (>1000 min)", 
         "SELECT count() FROM train_delays FINAL WHERE delay_in_min > 1000"),
         
        ("Range Check: Future Data (>7 Days)", 
         "SELECT count() FROM train_delays FINAL WHERE planned_departure > now() + INTERVAL 7 DAY"),

        ("Ref Integrity: Unknown Stations", 
         "SELECT count() FROM train_delays FINAL WHERE city = 'Unknown' AND timestamp > now() - INTERVAL 1 HOUR"),
    ]
    
    # Предупреждения (не роняют пайплайн)
    warning_checks = [
        ("Duplicates (pre-merge)",
         """SELECT count() FROM (
             SELECT train_id, planned_departure, city, count() as cnt 
             FROM train_delays
             WHERE timestamp > now() - INTERVAL 1 HOUR
             GROUP BY train_id, planned_departure, city 
             HAVING cnt > 1
         )"""),
    ]
    
    failed_checks = []
    
    for check_name, sql in critical_checks:
        try:
            result = client.query(sql).result_rows[0][0]
            if result > 0:
                msg = f"❌ DQ FAIL: {check_name} -> найдено {result} плохих записей"
                print(msg)
                failed_checks.append(msg)
            else:
                print(f"✅ DQ PASS: {check_name}")
        except Exception as e:
            print(f"⚠ Ошибка при выполнении проверки {check_name}: {e}")
            failed_checks.append(f"SQL Error in {check_name}: {e}")
    
    for check_name, sql in warning_checks:
        try:
            result = client.query(sql).result_rows[0][0]
            if result > 0:
                print(f"⚠ DQ WARNING: {check_name} -> {result} записей (будут дедуплицированы)")
            else:
                print(f"✅ DQ PASS: {check_name}")
        except Exception as e:
            print(f"⚠ Ошибка при выполнении проверки {check_name}: {e}")
    
    try:
        client.command("OPTIMIZE TABLE train_delays FINAL")
        print("✅ OPTIMIZE TABLE train_delays FINAL - дедупликация выполнена")
    except Exception as e:
        print(f"⚠ OPTIMIZE не выполнен: {e}")
            
    if failed_checks:
        raise Exception(f"Data Quality Checks Failed:\n" + "\n".join(failed_checks))

# ==========================================
# 4. TRANSFORM GOLD (Silver -> Aggregated)
# ==========================================
def transform_gold():
    print("🔨 TASK 4: TRANSFORM GOLD...")
    client = get_ch_client()
    
    # Стандарт Deutsche Bahn: поезд считается опоздавшим если задержка > 5 минут
    query = """
    INSERT INTO daily_train_stats
    SELECT
        toDate(planned_departure) as stat_date,
        city,
        train_type,
        count() as total_trains,
        countIf(delay_in_min > 5) as delayed_trains,
        avgIf(delay_in_min, delay_in_min > 5) as avg_delay,
        maxIf(delay_in_min, delay_in_min > 5) as max_delay,
        now() as created_at
    FROM train_delays FINAL
    WHERE planned_departure >= toStartOfDay(now() - INTERVAL 1 DAY)
      AND planned_departure < toStartOfDay(now() + INTERVAL 1 DAY)
    GROUP BY stat_date, city, train_type
    """
    
    # Удаляем старые данные за сегодня и вчера перед вставкой
    client.command("""
        ALTER TABLE daily_train_stats DELETE 
        WHERE stat_date >= toDate(now() - INTERVAL 1 DAY)
    """)
    
    client.command(query)
    print("✅ TRANSFORM: Gold слой (daily_train_stats) обновлен.")

# --- ORCHESTRATOR ---
async def run_pipeline(context):
    config = load_config()
    
    # 0. ENSURE TABLES EXIST
    try:
        ensure_clickhouse_tables()
    except Exception as e:
        log_status(context, "INIT", "FAILED", f"Cannot create tables: {e}")
        raise
    
    # 1. EXTRACT
    try:
        df = await extract_data(config)
        
        # Tech Check: API health
        total = len(df)
        failed_count = df['error'].notna().sum()
        
        if total == 0:
            raise Exception("CRITICAL: Не сгенерировано ни одного запроса.")
            
        if failed_count == total:
            raise Exception("CRITICAL: Все запросы к API упали.")
            
        success_rate = (total - failed_count) / total * 100
        print(f"📈 API Success Rate: {success_rate:.1f}% ({total - failed_count}/{total})")
        
        if failed_count > 0:
            print(f"⚠ WARNING: {failed_count}/{total} запросов с ошибкой.")
            
    except Exception as e:
        log_status(context, "EXTRACT", "FAILED", str(e))
        raise

    # 2. LOAD
    try:
        count = load_to_silver(df, config)
    except Exception as e:
        log_status(context, "LOAD", "FAILED", str(e))
        raise

    # 3. DQ CHECK
    if count > 0:
        try:
            data_quality_check()
        except Exception as e:
            log_status(context, "DQ_CHECK", "FAILED", str(e))
            raise

        # 4. TRANSFORM
        try:
            transform_gold()
        except Exception as e:
            log_status(context, "TRANSFORM", "FAILED", str(e))
            raise
            
    log_status(context, "PIPELINE", "SUCCESS", f"Processed {count} records")

def main(**kwargs):
    asyncio.run(run_pipeline(kwargs))