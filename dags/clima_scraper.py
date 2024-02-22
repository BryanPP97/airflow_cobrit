from datetime import datetime, timedelta
import pendulum
import pandas as pd
from sqlalchemy import create_engine
from utils.utils_general import clean_folder
from airflow.hooks.S3_hook import S3Hook

#import scrapers
from utils.clima import clima_scraper
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Python operator to run scripts
from airflow.operators.python_operator import PythonOperator

# Configuration
default_args={
    'owner': 'cobrit',
    "depends_on_past": False,
    #"email": ["jmontan@coperva.com"],
    #"email_on_failure": True,
    #"email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=1),
}

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('S3-Connection')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)

## Create DAG
local_tz = pendulum.timezone("America/Mexico_City")
with DAG(
    "clima_scraper",
    default_args=default_args,
    description="Download clima data",
    start_date= datetime(year=2024, month=2, day=22, tzinfo=local_tz),
    #schedule_interval="0 9 * * 1-5",
    schedule_interval="@monthly",
    tags = ['clima', 'data', 'scraper']
) as dag:
    cleaner = PythonOperator(
        task_id="cleaner",
        python_callable=clean_folder,
        op_kwargs={'folder': '/opt/airflow/outputs/clima/'}
    )
    download = PythonOperator(
        task_id="download", 
        python_callable=clima_scraper
    )
    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/opt/airflow/outputs/clima/datos_climaticos.csv',
            'key': 'datos_climaticos.csv',
            'bucket_name': 'emi-data'
        }
    )
    cleaner >> download >> upload_to_s3 