
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import hashlib
import pandas as pd
import random

PATH = '/opt/synthetic_data'

#не збудь про скрепинг данных по api

""" В Apache Airflow объект default_args используется для задания общих параметров
    по умолчанию для всех задач (tasks) в DAG.
    Это позволяет избежать дублирования кода и упрощает управление настройками задач. 
    Параметры, заданные в default_args, применяются ко всем задачам в DAG,
    если они не переопределены на уровне конкретной задачи.   """

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(weeks=1),
    'retries': 1
}

"""  shedule_interval = "/15 * * * * "  - каждые 15 минут   """
dag = DAG( default_args=default_args,
           dag_id="data_synth",
           schedule_interval="/15 * * * *", # как часто будет запускаться 
        )



ROWS = 200
def generate_card(execution_data):
    card_data = []
    for i in range(1000):
        card_num = "".join(random.choices("0123456789", k=16))
        card_code = hashlib.md5(card_num.encode()).hexdigest()
        url = f"http:/some_domen/page{random.randint(1,10)}"
        cookie = f"session_{random.randint(10000, 99999)}"
        load_date = execution_data
        card_data.append({

            "card_num": card_num,
            "card_code_m5" : card_code,
            "cookie" : cookie,
            "load_date":load_date
        })

    pd = pd.Dataframe(card_data)
    card_data.to_csv(f'{PATH}/cards_{execution_data}.csv', index=False, sep=';')



    
def generate_status(execution_data):
    cards_df = pd.read_csv(f"{PATH}/cards_{execution_data}.csv", sep=";")
    status_data = []
    for i in range(ROWS):
        status = random.choice["выдана", "не выдана"]
        if status == "выдана":
            row = cards_df.sample().iloc[0] # sample() - случайная строка в cards_df
            card_num = row['card_num']
            card_code = row['card_num_md5']
            datetime_status = datetime(execution_data, '%Y-%M-%d') - timedelta(DAYS=random.randint(1, 10))
            if datetime_status <= datetime.strptime(row['load_date'], '%Y-%m-%d'):
                status_data.append({
            
                    'status': status,
                    'datetime': datetime_status.strftime('%Y-%m-%d %H:%M:%S'), #ГГГГ-ММ-ДД ЧЧ:ММ:СС
                    'card_num': card_num,
                    'card_num_md_5': card_code,
                    'load_date' : row['load_date']

                }
                )               
        
        else:
            card_num = "".join(random.choices("0123456789", k=16))
            card_date = hashlib.md5(card_num.encode()).hexdigest()
            datetime_status = datetime.strftime(execution_data, '%Y-%M-%d')  -  timedelta(days=random.randint(1, 10))
            card_code = hashlib.md5(card_num.encode()).hexdigest() # card_num.encode() - преобразует строку card_num в байтовый формат
            # вычисляет MD-5 ХЕШ и возвращает в виде шестнадцатеричной строки (hex)
            
            load_date = execution_data

            status_data.append({
                    'status': status,
                    'datetime': datetime_status,
                    'card_num': card_num,
                    'card_num_md_5': card_code,
                    'load_date' : load_date
            })
    cards_df.drop(columns='card_num').to_csv(f"{PATH}/cards_{execution_data}.csv", index=False, sep=";")
    status_df = pd.DataFrame(status_data)
    status_df.to_csv(f"{PATH}/cards_status_{execution_data}.csv", index=False, sep=";")

def generate_transaction(execution_data):
    df_status_card = pd.read_csv(f"{PATH}/cards_{execution_data}.csv", sep=";")
    df_generate_trans = []
    for index, rows in df_status_card.iterrows():
        if df_status_card['status'] == "выдана":
            sum_transaction = round(random.uniform(10.0, 5000.0), 2)
            num_transaction = random.randint(1, 10)
            for i in range(num_transaction):
                date_transaction = datetime.strptime(rows['datetime'], '%Y-%m-%d %H:%M:%S') + timedelta(days=random.randint(1, 100))

                load_date = rows['load_date']

                if  date_transaction <= datetime.strptime(load_date, '%Y-%m-%d'):  # Условие для транзакции
                     date_transaction.append({
                        'card_num': rows['card_num'],
                        'amount': sum_transaction,
                        'transaction_datetime':  date_transaction.strftime('%Y-%m-%d %H:%M:%S'),
                        'load_date': load_date
                    })

        transaction_df = pd.DataFrame(date_transaction)
        transaction_df.to_csv(f'{PATH}/transactions_{date_transaction}.csv', index=False, sep=';')



synth_card = PythonOperator(
    task_id = 'card',
    python_callable=generate_card,
    op_kwargs={'execution_date': '{{ ds }}'},  # Передаем execution_date
    dag=dag
)

synth_status = PythonOperator(
    task_id = 'status',
    python_callable=generate_status,
    op_kwargs={'execution_date': '{{ ds }}'},  # Передаем execution_date
    dag=dag
)

synth_transaction = PythonOperator(
    task_id = 'transaction',
    python_callable=generate_transaction,
    op_kwargs={'execution_date': '{{ ds }}'},  # Передаем execution_date
    dag=dag
)

synth_card >> synth_status >> synth_transaction

