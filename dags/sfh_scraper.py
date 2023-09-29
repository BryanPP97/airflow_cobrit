from datetime import datetime, timedelta
import pendulum

#import scrapers
from utils.SHF import ift_scraper, pdf_convert, extractor_tablas
from utils.utils_general import clean_folder
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
    "SHF_Scraper",
    default_args=default_args,
    description="Download shf data",
    start_date= datetime(year=2023, month=9, day=27, tzinfo=local_tz),
    schedule_interval="@monthly",
    tags = ['sms']
) as dag:
    scraper = PythonOperator(
        task_id="scraper", 
        python_callable=ift_scraper
        )
    cleaner = PythonOperator(
        task_id="cleaner",
        python_callable=clean_folder,
        op_kwargs={'folder': '/opt/airflow/outputs/shf/'}
    )
    converter = PythonOperator(
        task_id="converter",
        python_callable=pdf_convert
    )
    t_maker = PythonOperator(
        task_id="table_maker",
        python_callable=extractor_tablas
    )

    cleaner >> scraper >> converter >> t_maker