from datetime import datetime
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException


def check_data():
    # Логика проверки
    if data_is_bad:
        raise AirflowException("Данные повреждены, остановка DAG")


@task
def format_downloaded_files_for_telegram(files: dict) -> str:
    if not files:
        return '⚠️ *{today} на FTP-сервере не было новых файлов*'

    today = datetime.today().strftime('%Y-%m-%d')
    header = f'✅ *{today} с FTP-сервера скачены новые файлы:*\n\n'
    
    lines = []
    for file_path in files.keys():
        file_name = file_path.split('/')[-1]
        # Экранируем подчеркивания, так как они часто встречаются в именах файлов
        # и воспринимаются классическим Markdown как начало курсива
        lines.append(f'• `{file_name}`')
    
    return header + '\n'.join(lines)


@task
def format_not_validated_files_for_telegram(files: list) -> str:
    today = datetime.today().strftime('%Y-%m-%d')
    if not files:
        raise AirflowSkipException('Нет файлов, не прошедших валидацию')
    header = f'❌ *{today} не удалась валидация следующих файлов:*\n\n'

    lines = []
    for file_path in files:
        file_name = file_path.split('/')[-1]
        lines.append(f'• `{file_name}`')
    
    return header + '\n'.join(lines)

def on_failure_telegram(context):
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M:%S')
    log_url = ti.log_url
    
    exception = context.get('exception')
    # В Markdown символы типа _ или * в тексте ошибки могут сломать верстку, 
    # поэтому оборачиваем их в блок кода
    error_msg = str(exception)[:500] if exception else "No exception info found"

    # Форматирование Markdown (старый стиль)
    message = (
        f"🔴 *Task Failed*\n\n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Time:* {execution_date}\n"
        f"*Error:*\n```\n{error_msg}\n```\n"
    )

    hook = TelegramHook(telegram_conn_id='telegram_notifications_conn')
    hook.send_message({
        'text': message,
        'parse_mode': 'Markdown', # Меняем на Markdown
        'disable_web_page_preview': True
    })
