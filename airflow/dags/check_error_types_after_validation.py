import os
import pandas as pd
from datetime import date, datetime
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator

# Настройки
DIRECTORY = '/smb/airflow/facts_data/errors'

EXCEPTIONS = [
    {'validator': 'validate_in_unrecognized', 'column': 'id_sku', 'error_value': 'ИСКЛЮЧИТЬ'},
    {'aptadress_ish_vis_ru': 'БРАК (НА ОБМЕН У ПОСТАВЩИКОВ ЗР)'},
    {'aptadress_ish_vis_ru': 'БРАК (НА ОБМЕН У ПОСТАВЩИКОВ ИФ) УЛ. НАДЕЖДИНСКАЯ'}
]

@dag(
    schedule_interval='0 5 * * *',  # Каждый день в 8 утра
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['monitoring']
)
def check_errors_reporting():

    @task
    def generate_report():
        today = date.today()
        files_with_errors = []

        if not os.path.exists(DIRECTORY):
            return f"❌ Ошибка: Путь {DIRECTORY} недоступен."

        for filename in os.listdir(DIRECTORY):
            full_path = os.path.join(DIRECTORY, filename)
            
            if os.path.isfile(full_path):
                mtime_date = datetime.fromtimestamp(os.path.getmtime(full_path)).date()
                
                if mtime_date == today:
                    try:
                        df = pd.read_csv(full_path, encoding='cp1251', sep=';', low_memory=False)
                        if df.empty: continue

                        is_error = pd.Series([True] * len(df))
                        for rule in EXCEPTIONS:
                            rule_mask = pd.Series([True] * len(df))
                            for col, val in rule.items():
                                if col in df.columns:
                                    rule_mask &= (df[col] == val)
                                else:
                                    rule_mask &= False
                            is_error &= ~rule_mask

                        if is_error.any():
                            files_with_errors.append(filename)
                    except Exception as e:
                        print(f"Ошибка в файле {filename}: {e}")

        if files_with_errors:
            # Используем HTML теги <b> и <code> вместо Markdown
            header = "⚠️ <b>В ходе проверки невалидных строк были обнаружены случаи, требующие ручной проверки (/smb/airflow/facts_data/errors):</b>\n"
            file_list = "\n".join([f"• <code>{f}</code>" for f in files_with_errors])
            return header + file_list
        return "✅ Ошибок в невалидных данных (/smb/airflow/facts_data/errors) за сегодня не обнаружено."

    # Вызываем таску генерации
    report_text = generate_report()

    # Передаем результат напрямую в TelegramOperator
    send_to_tg = TelegramOperator(
        task_id='send_report_tg',
        telegram_conn_id='telegram_notifications_conn',
        text=report_text,
        telegram_kwargs={
          'parse_mode': 'HTML'
        }
    )

    report_text >> send_to_tg

# Инициализация DAG
check_errors_reporting_dag = check_errors_reporting()
