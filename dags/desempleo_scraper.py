## Import time modules
from datetime import datetime, timedelta
import pendulum

from airflow import DAG

#import scrapers
from utils.INEGI_desempleo import INEGI_desempleo_scraper, data_transformation
from utils.utils_general import clean_folder, upload_to_s3

# Python operator to run scripts
from airflow.operators.python_operator import PythonOperator

# Configuration
default_args={
    'owner': 'cobrit',
    "depends_on_past": False,
    "email": ["jmontan@coperva.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

## Define local time
local_tz = pendulum.timezone("America/Mexico_City")

with DAG(
    "Desempleo_Scraper",
    default_args=default_args,
    description="Download desempleo data from INEGI",
    start_date= datetime(year=2023, month=10, day=1, tzinfo=local_tz),
    schedule_interval="@monthly",
    tags = ['scraper', 'data']
) as dag:
    cleaner = PythonOperator(
        task_id="cleaner",
        python_callable=clean_folder,
        op_kwargs={'folder': '/opt/airflow/outputs/'}
    )
    scraper = PythonOperator(
        task_id="scraper", 
        python_callable=INEGI_desempleo_scraper
    )
    transformer = PythonOperator(
        task_id="transformer",
        python_callable=data_transformation
    )
    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/opt/airflow/outputs/Tabulado.csv',
            'key': 'Tabulado.csv',
            'bucket_name': 'emi-data'
        }
    )
    cleaner >> scraper >> transformer >> upload_to_s3