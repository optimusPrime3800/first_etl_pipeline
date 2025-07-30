from datetime import datetime, timedelta
from airflow import DAG     
from airflow.providers.
from airflow.operators.bash import BashOperator
from hashlib import md5
from sqlalchemy import create_engine
import pandas as pd


default_args = {
    'owner_airflow': 'amir',
    'start_date': datetime(2025, 2, 1),
    'retries': 1
}

dag = DAG(
    dag_id="monitoring",
    default_args=default_args,
    schedule_interval=''
)

load_data = BashOperator(
    task_id = 'load_to_click',
    bash_command = 'cd /opt/dbt_click && dbt run --target monitoring -m monitoring',
    dag=dag
)

load_data

