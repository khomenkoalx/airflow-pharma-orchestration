from datetime import datetime, timedelta
from pathlib import Path
import os
import shutil
import uuid

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd

from utils.ftp_utils import download_file_from_ftp
from utils.telegram_utils import on_failure_telegram

ftp_conn_id = 'ftp_conn'
postgres_conn_id = 'postgres_conn'
file_path = '/Выгрузки/АДРЕСА.csv'
local_path = '/smb/airflow/address_data/input/АДРЕСА.csv'
processed_files_folder = '/smb/airflow/address_data/processed'
SCHEMA_NAME = 'dev'
TABLE_NAME = 'dim_address'

UPDATE_DIM_PHARMACY_TABLE = f"""
    INSERT INTO {SCHEMA_NAME}.dim_pharmacy (id_pharmacy, id_client, tin_pharmacy, id_fias, valid_from, valid_to)
    SELECT 
        id_pharmacy,
        id_client,
        tin_pharmacy,
        id_fias,
        MIN(operation_date) AS valid_from,
        MAX(operation_date) AS valid_to
    FROM {SCHEMA_NAME}.stg_dim_pharmacy sdp 
    GROUP BY id_pharmacy, id_client, id_fias, tin_pharmacy
    ON CONFLICT (id_pharmacy, id_client, valid_from) 
    DO UPDATE SET 
        tin_pharmacy = EXCLUDED.tin_pharmacy,
        id_fias      = EXCLUDED.id_fias,
        valid_to     = EXCLUDED.valid_to;
    TRUNCATE TABLE {SCHEMA_NAME}.stg_dim_pharmacy;
"""





with DAG(
    dag_id='process_pharmacies_and_addresses',
    start_date=datetime(2026, 1, 1),
    schedule_interval='30 2 */1 * *',
    catchup=False,
    tags=['get_data'],
    on_failure_callback=on_failure_telegram
) as dag:

    @task
    def download_address_csv_task(retries=3, retry_delay=timedelta(minutes=5)):
        hook = FTPHook(ftp_conn_id=ftp_conn_id)
        with hook.get_conn() as ftp:
            ftp.encoding = 'utf-8'
            download_file_from_ftp(ftp, file_path, local_path)

    @task
    def get_ids_fias_and_load_to_db_task():
        df = pd.read_csv(local_path, encoding='cp1251', sep='\t')
        df = df[~df['fias_code_vis_ru'].isin(['НЕ ОПРЕДЕЛЕНО', '~', 'ТЕРРИТОРИЯ ВНЕ КЛАССИФИКАТОРА'])]
        
        # Конвертируем в список строк
        unique_id_fias = [str(x) for x in df['fias_code_vis_ru'].unique()]
    
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        
        # Работаем напрямую с курсором psycopg2
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = f"""
                    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (id_fias)
                    SELECT UNNEST(%s::uuid[])
                    ON CONFLICT (id_fias) DO NOTHING
                """
                # В psycopg2 список Python автоматически мапится в массив Postgres
                cur.execute(query, (unique_id_fias,))
                conn.commit()
                print(f'Rows affected: {cur.rowcount}')


    @task
    def get_dim_pharmacy_and_load_to_db_stg_task():
        df = pd.read_csv(local_path, encoding='cp1251', sep='\t')
        df['month_ru'] = pd.to_datetime(df['month_ru'])
        df['net_id_vis_vis_ru'] = df['net_id_vis_vis_ru'].replace('~', '10003')
        df = df[~df['fias_code_vis_ru'].isin(['НЕ ОПРЕДЕЛЕНО', '~', 'ТЕРРИТОРИЯ ВНЕ КЛАССИФИКАТОРА'])]
        df = df[~df['apt_inn_vis_ru'].isin(['НЕ ОПРЕДЕЛЕНО', '~'])]
        df = df.rename(
          columns={
            'month_ru': 'operation_date',
            'net_id_vis_vis_ru': 'id_client',
            'apt_inn_vis_ru': 'tin_pharmacy',
            'fias_code_vis_ru': 'id_fias',
            'apt_id_vis_ru': 'id_pharmacy'
          })
        df = df[['operation_date', 'id_client', 'tin_pharmacy', 'id_fias', 'id_pharmacy']]
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        engine = hook.get_sqlalchemy_engine()
        with hook.get_conn() as conn:
            df.to_sql(
                name='stg_dim_pharmacy',
                con=engine,
                schema=SCHEMA_NAME,
                if_exists='append',
                index=False
            )

    update_dim_pharmacy_task = SQLExecuteQueryOperator(
        task_id='update_dim_pharmacy',
        conn_id=postgres_conn_id,
        sql=UPDATE_DIM_PHARMACY_TABLE,
        split_statements=True
    )

    @task
    def move_file_task():
        dest_path = Path(processed_files_folder) / 'АДРЕСА.csv'
        if dest_path.exists():
            os.remove(dest_path)
        shutil.move(local_path, dest_path)


  
    download = download_address_csv_task()
    insertion = get_ids_fias_and_load_to_db_task()
    load_to_dim_pharmacy = get_dim_pharmacy_and_load_to_db_stg_task()
    move_file = move_file_task()

    download >> [insertion, load_to_dim_pharmacy]
    insertion >> move_file
    load_to_dim_pharmacy >> update_dim_pharmacy_task >> move_file
