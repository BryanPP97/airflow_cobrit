from datetime import datetime, timedelta
import pendulum

#import scrapers
from utils.audios_cc1 import cc1_Nu
from utils.utils_general import clean_folder, send_single_email
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Python operator to run scripts
from airflow.operators.python_operator import PythonOperator

# Configuration
default_args={
    'owner': 'cobrit',
    "depends_on_past": False,
    "email": ["ia2@coperva.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
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
    "ccc1_NU",
    default_args=default_args,
    description="Sends email for audio data",
    start_date= datetime(year=2024, month=2, day=12, tzinfo=local_tz),
    #schedule_interval="0 9 * * 1-5",
    schedule_interval="0 9-18 * * 1-5",
    tags = ['ccc1', 'audios', 'Nu']
) as dag:
    cleaner = PythonOperator(
        task_id="cleaner",
        python_callable=clean_folder,
        op_kwargs={'folder': '/opt/airflow/audios/'}
    )

    download = PythonOperator(
        task_id="download", 
        python_callable=cc1_Nu,
    )
    send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_single_email,
    op_kwargs={
        'to_email': 'polito.bryan@gmail.com',
        'subject': 'Audios Nu',
        'body': 'Este es un correo de prueba',
        'attachment': '/opt/airflow/audios'
    }
    )

    download >> send_email_task