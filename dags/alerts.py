import requests
import os

def send_telegram_alert(context):
    """
    Эта функция вызывается Airflow автоматически при ошибке.
    """
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not token or not chat_id:
        print("❌ Телеграм токен не найден, алерт не отправлен.")
        return

    # Достаем детали ошибки из контекста Airflow
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    log_url = task_instance.log_url

    message = (
        f"🔴 **Airflow Alert** 🔴\n\n"
        f"❌ **DAG:** `{dag_id}`\n"
        f"🔧 **Task:** `{task_id}`\n"
        f"📅 **Date:** `{execution_date}`\n\n"
        f"📄 **Error:** `{str(exception)[:200]}...`\n\n"
        f"[Посмотреть логи]({log_url})"
    )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        requests.post(url, data=payload)
        print("✅ Алерт отправлен в Telegram.")
    except Exception as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")