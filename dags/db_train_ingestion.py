# db_train_ingestion.py 

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь к скриптам, чтобы Airflow мог их импортировать
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))
from train_generator import run_simulation
from db_real_ingestion import main as run_real_api_ingestion

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'db_train_simulation',
    default_args=default_args,
    description='Симуляция подкачки данных DB каждые 10 минут',
    schedule_interval=timedelta(minutes=10), # Интервал запуска
    catchup=False # Не запускать за прошлые периоды
) as dag:

    ingest_task = PythonOperator(
        task_id='real_api_ingestion',
        python_callable=run_simulation,
    )

    ingest_task