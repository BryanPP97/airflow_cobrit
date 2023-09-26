from datetime import datetime, timedelta
import pendulum

#import scrapers
from utils.gepard import gepard_automation, process_sms
from utils.utils_general import email, clean_folder
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'trigger_rule': 'all_success'
}

## Create DAG
local_tz = pendulum.timezone("America/Mexico_City")
with DAG(
    "SMS_GEPARD",
    default_args=default_args,
    description="Sends email for positive sms using gepard data",
    start_date= datetime(year=2023, month=9, day=21, tzinfo=local_tz),
    schedule_interval="0 9 * * 1-5",
    tags = ['sms']
) as dag:
    scraper = PythonOperator(
        task_id="scraper_gepard", 
        python_callable=gepard_automation
        )
    processor = PythonOperator(
        task_id='processor',
        python_callable=process_sms
    )
    mailing = PythonOperator(
        task_id="mailing", 
        python_callable=email,
        op_kwargs={'portal': 'Gepard'}
    )
    cleaner = PythonOperator(
        task_id="cleaner",
        python_callable=clean_folder,
        op_kwargs={'folder': '/opt/airflow/outputs/Gepard/'}
    )

    scraper >> processor >> mailing >> cleaner