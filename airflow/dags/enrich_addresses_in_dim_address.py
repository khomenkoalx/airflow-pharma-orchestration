from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator


with DAG(
    dag_id='enrich_addresses',
    start_date=datetime(2026, 1, 1),
    schedule_interval='15 3 */1 * *',
    catchup=False,
    tags=['addresses', 'db']
) as dag:
  

    validate_task = DockerOperator(
        task_id='enrich_addresses',
        image='visanalytics/etl-toolbox:latest',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        force_pull=True,
        docker_conn_id='docker_registry',
        environment={
            'DADATA_TOKEN': Variable.get('DADATA_TOKEN'),
            'DADATA_SECRET': Variable.get('DADATA_SECRET'),
            'DB_CONNECTION_STRING_SECRET': Variable.get('DB_CONNECTION_STRING_SECRET'),
            'EMAIL_PASSWORD': Variable.get('EMAIL_PASSWORD'),
            'EMAIL_RECEIVER': Variable.get('EMAIL_RECEIVER'),
            'EMAIL_SENDER': Variable.get('EMAIL_SENDER'),
            'SMTP_HOST': Variable.get('SMTP_HOST'),
            'SMTP_PORT': Variable.get('SMTP_PORT')          
        },
        
        # Если нужно передать команду (если отличается от CMD образа)
        command='python -m enrich_addresses',
        
        # Дополнительные параметры
        network_mode='bridge',
        tty=False,  # -t флаг (но в Airflow обычно не нужно)
        
        # Для отладки можно увеличить timeout
        timeout=3600
    )
