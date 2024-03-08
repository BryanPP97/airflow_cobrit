from datetime import datetime, timedelta
import pendulum

#import scrapers
from utils.IFT import ift_automation
from utils.utils_general import clean_folder, upload_to_s3
from airflow import DAG

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

local_tz = pendulum.timezone("America/Mexico_City")
with DAG(
    "IFT_Scraper",
    default_args=default_args,
    description="Download ift data",
    start_date= datetime(year=2023, month=9, day=29, tzinfo=local_tz),
    schedule_interval="@monthly",
    tags = ['scraper', 'data']
) as dag:
    scraper = PythonOperator(
        task_id="scraper", 
        python_callable=ift_automation
        )
    cleaner = PythonOperator(
        task_id="cleaner",
        python_callable=clean_folder,
        op_kwargs={'folder': '/opt/airflow/output/'}
    )
    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/opt/airflow/outputs/ift.csv',
            'key': 'ift.csv',
            'bucket_name': 'emi-data'
        }
    )

    cleaner >> scraper >> upload_to_s3