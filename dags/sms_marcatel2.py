from datetime import datetime, timedelta
import pendulum
#import scrapers
#from airflow.sensors.external_task import ExternalTaskSensor
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

## Create DAG
local_tz = pendulum.timezone("America/Mexico_City")
with DAG(
    "Marcatel_mail",
    default_args=default_args,
    description="Sends email for positive sms using marcatel data",
    start_date= datetime(year=2023, month=9, day=25, tzinfo=local_tz),
    schedule_interval="0 9 * * 1-5",
    tags = ['sms']
) as dag:
    #sensor = ExternalTaskSensor(
    #    task_id='sensor_data',
    #    external_dag_id='Marcatel_download',
    #    external_task_id='processor'
    #)
    mailing = PythonOperator(
        task_id='mailing',
        python_callable=email,
        op_kwargs={'portal': 'Marcatel'}
    )

    mailing