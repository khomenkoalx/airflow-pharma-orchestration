from datetime import datetime, timedelta
import time
from pathlib import Path
import pandas as pd
import shutil
from sqlalchemy import create_engine
import os

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator


from utils.ftp_utils import get_current_files_on_ftp, download_file_from_ftp
from utils.telegram_utils import (
    format_downloaded_files_for_telegram,
    format_not_validated_files_for_telegram,
    on_failure_telegram
)

conn_id = 'ftp_conn'
root_dir = '/Выгрузки'

FILE_TO_DB_TABLE_MAPPING = {
  'ОСТАТКИ ДБ': 'stg_fct_rest',
  'ПРОДАЖИ ДБ': 'stg_fct_sale_second',
  'ТРАНЗИТ ДБ': 'stg_fct_rest',
  'ВОЗВРАТЫ ДБ': 'stg_fct_sale_second',
  'ОСТАТКИ': 'stg_fct_rest',
  'ЗАКУПКИ': 'stg_fct_purchase',
  'ПРОДАЖИ': 'stg_fct_sale_third'
}

SCHEMA_NAME = 'dev'

UPDATE_TABLES_IN_DB_QUERY = """
   DELETE FROM dev.fct_purchase sfp 
   WHERE file_name IN (SELECT file_name FROM dev.stg_fct_purchase sfp2 );
   DELETE FROM dev.fct_sale_second fss 
   WHERE file_name IN (SELECT file_name FROM dev.stg_fct_sale_second sfss  );
   DELETE FROM dev.fct_sale_third fst 
   WHERE file_name IN (SELECT file_name FROM dev.stg_fct_sale_third sfst  );
   DELETE FROM dev.fct_rest fr 
   WHERE file_name IN (SELECT file_name FROM dev.stg_fct_rest sfr  );
   
   INSERT INTO dev.fct_rest SELECT * FROM dev.stg_fct_rest;
   INSERT INTO dev.fct_sale_second SELECT * FROM dev.stg_fct_sale_second;
   INSERT INTO dev.fct_sale_third SELECT * FROM dev.stg_fct_sale_third;
   INSERT INTO dev.fct_purchase SELECT * FROM dev.stg_fct_purchase;
   
   TRUNCATE TABLE dev.stg_fct_purchase;
   TRUNCATE TABLE dev.stg_fct_sale_second;
   TRUNCATE TABLE dev.stg_fct_sale_third;
   TRUNCATE TABLE dev.stg_fct_rest;
"""

MAIN_DIR = '/smb/airflow/facts_data'

with DAG(
    dag_id='main_pharmacy_data_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval='15 1 */1 * *',
    catchup=False,
    tags=['get_data'],
    on_failure_callback=on_failure_telegram,
    max_active_tasks=4
) as dag:
  
    @task
    def get_ftp_file_list():
        hook = FTPHook(ftp_conn_id=conn_id)
        with hook.get_conn() as ftp:
            ftp.encoding = 'utf-8'
            files_dict = get_current_files_on_ftp(ftp, root_dir)
            return files_dict
    
    @task(
        max_active_tis_per_dag=4,
        execution_timeout=timedelta(minutes=10),
        retries=3,
        retry_delay=timedelta(seconds=30)
    )
    def download_single_file_task(file_path_and_mdtm):
        file_path = file_path_and_mdtm[0]
        mdtm = file_path_and_mdtm[1]
        hook = FTPHook(ftp_conn_id=conn_id)
        with hook.get_conn() as ftp:
            ftp.encoding = 'utf-8'
            
            filename = file_path.split('/')[-1].split('.')[-2] + f'_{mdtm}.csv'
            local_path = f'{MAIN_DIR}/input/{filename}'
            
            print(f"Запуск отдельной задачи для файла: {file_path}")
            download_file_from_ftp(ftp, file_path, local_path)
            time.sleep(5)
    
    @task
    def fetch_file_names_and_mdtd_from_db():
        hook = PostgresHook(postgres_conn_id='postgres_conn')
    
        query = """
          SELECT file_name, mdtm FROM dev.fct_purchase
          UNION
          SELECT file_name, mdtm FROM dev.fct_sale_second
          UNION
          SELECT file_name, mdtm FROM dev.fct_sale_third
          UNION
          SELECT file_name, mdtm FROM dev.fct_rest
        """
        df = hook.get_pandas_df(query)
        
        result = df.set_index('file_name')['mdtm'].to_dict()
        print(result)
        return result
    
    @task
    def filter_files_to_download(file_paths_in_ftp, files_in_db):
        files_in_ftp_to_download = {}
        for ftp_path, ftp_mdtm in file_paths_in_ftp.items():
            filename = ftp_path.split('/')[-1]
            if filename in files_in_db:
                if files_in_db[filename] != ftp_mdtm:
                    files_in_ftp_to_download[ftp_path] = ftp_mdtm
            elif 'адреса' in filename.lower():
                continue
            else:
                files_in_ftp_to_download[ftp_path] = ftp_mdtm
        print('Грузим: ', files_in_ftp_to_download)
        return files_in_ftp_to_download
        
    @task
    def truncate_path_from_files(file_paths_in_ftp):
        files_with_mdtm = {ftp_path.split('/')[-1]: mdtm for ftp_path, mdtm in file_paths_in_ftp.items()}
        print(files_with_mdtm)
        return files_with_mdtm
    
    
    validate_task = DockerOperator(
        task_id='validate',
        image='visanalytics/etl-toolbox:latest',
        api_version='auto',
        auto_remove=True,  # соответствует --rm
        docker_url='unix://var/run/docker.sock',
        force_pull=True,  # соответствует --pull always
        docker_conn_id='docker_registry',
        mounts=[
            {
                'source': f'{MAIN_DIR}',
                'target': '/app/data',
                'type': 'bind'
            }
        ],
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
        command='python -m validate',
        
        network_mode='bridge',
        tty=False, 
        
        timeout=3600,
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    @task
    def list_files_in_validated_and_set_destination_table():
        dir_path = Path(f'{MAIN_DIR}/validated')
        files_in_validated = [str(f) for f in dir_path.iterdir() if f.is_file()]
        
        file_to_destination_table_mapping = {}
        
        sorted_masks = sorted(FILE_TO_DB_TABLE_MAPPING.keys(), key=len, reverse=True)

        for file in files_in_validated:
            file_path = Path(file)
            file_name = file_path.name
            
            for file_mask in sorted_masks:
                if file_mask in file_name and file not in file_to_destination_table_mapping:
                    file_to_destination_table_mapping[file] = FILE_TO_DB_TABLE_MAPPING[file_mask]
                    break
        return file_to_destination_table_mapping

    @task
    def load_csv_to_postgres_and_move_file(file_to_table_mapping):
        destination_folder = Path(f'{MAIN_DIR}/loaded')
        file_path = Path(file_to_table_mapping[0])
        table_name = file_to_table_mapping[1]
      
        mdtm = file_path.stem.split('_')[-1]
        file_name = '_'.join(file_path.stem.split('_')[:-1]) + '.csv'

        df = pd.read_csv(file_path, sep=';', decimal=',')
        df['file_name'] = file_name
        df['mdtm'] = mdtm
        print(f'Загружаю файл: {file_name} с меткой {mdtm} в таблицу {table_name}')

        hook = PostgresHook(postgres_conn_id='postgres_conn')

        conn_uri = hook.get_uri().replace('postgres://', 'postgresql+psycopg2://')
        engine = create_engine(conn_uri)



        df.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA_NAME,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=50000
        )
        dest_path = destination_folder / file_path.name
        if dest_path.exists():
            os.remove(dest_path)
        shutil.move(file_path, destination_folder)
        print(f"Успешно загружено {len(df)} строк.")

    update_tables_in_db_task = SQLExecuteQueryOperator(
        task_id='update_tables_in_db',
        conn_id='postgres_conn',
        sql=UPDATE_TABLES_IN_DB_QUERY,
        split_statements=True
    )

    @task
    def list_input_dir():
        dir = f'{MAIN_DIR}/input'
        return os.listdir(dir)
            

    file_paths_in_ftp = get_ftp_file_list()
    files_in_db = fetch_file_names_and_mdtd_from_db()

    files_in_ftp_to_download = filter_files_to_download(file_paths_in_ftp, files_in_db)

    downloaded_files = download_single_file_task.expand(file_path_and_mdtm=files_in_ftp_to_download)

    downloaded_files >> validate_task
    
    files_to_load = list_files_in_validated_and_set_destination_table()
    validate_task >> files_to_load
    
    loaded_files = load_csv_to_postgres_and_move_file.expand(file_to_table_mapping=files_to_load)
    files_to_load >> loaded_files
    
    loaded_files >> update_tables_in_db_task

    
    not_validated_files = list_input_dir()

    processed_files_notification = TelegramOperator(
        task_id='processed_files_notification',
        telegram_conn_id='telegram_notifications_conn',
        text=format_downloaded_files_for_telegram(files_in_ftp_to_download),
        telegram_kwargs={
            'parse_mode': 'Markdown'
        }
    )
  
    not_validated_notification = TelegramOperator(
        task_id='not_validated_notification',
        telegram_conn_id='telegram_notifications_conn',
        text=format_not_validated_files_for_telegram(not_validated_files),
        telegram_kwargs={
            'parse_mode': 'Markdown'
        }
    )

    loaded_files >> processed_files_notification
    validate_task >> not_validated_files >> not_validated_notification
