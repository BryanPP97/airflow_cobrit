from datetime import datetime, timedelta
import pendulum
#import scrapers
from utils.marcatel import marcatel_automation, process_sms
from utils.utils_general import clean_folder
# The DAG object; we'll need this to instantiate a DAG
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
    "retry_delay": timedelta(minutes=5),
}

## Create DAG
local_tz = pendulum.timezone("America/Mexico_City")
with DAG(
    "Marcatel_download",
    default_args=default_args,
    description="Download data from Marcatel portal",
    start_date= datetime(year=2023, month=9, day=28, tzinfo=local_tz),
    schedule_interval="0 23 * * 1-5",
    tags = ['sms']
) as dag:
    scraper = PythonOperator(
        task_id="scraper_marcatel", 
        python_callable=marcatel_automation
        )
    processor = PythonOperator(
        task_id='processor',
        python_callable=process_sms
    )
    cleaner = PythonOperator(
        task_id="cleaner",
        python_callable=clean_folder,
        op_kwargs={'folder': '/opt/airflow/outputs/Marcatel/'}
    )

    cleaner >> scraper >> processor