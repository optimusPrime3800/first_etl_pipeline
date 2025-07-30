from airflow import DAG
import datetime
import random 
import datetime
import time
dags_args = {
    'owner': 'maktra*er',
    'start_date': datetime(2025, 6, 11),
    'end_date': datetime(2025, 12, 12),
    'retry_delay': time.timedelta(hours=2)
}

dag = DAG(
    dag_id = 'postgre_extract',
    default_arg=dags_args,
    description='пока не придумал',
    catchup= False
)



def generate_app_installs(**kwargs):
    installs = []
    os_variants = ['ios', 'android']
    for _ in range(10):
        install = []
        install.append(
            {
                'id_os': random.randint(1000, 9999),
                'name_os': random.choice(os_variants),
                "time_current": datetime.now().isoformat()
            }
            )
        installs.append(install)
        kwargs['ti'].xcom_push(key_installs='installs', value=installs)