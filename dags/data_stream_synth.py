from airflow import DAG
from datetime import datetime, timedelta
import psycopg2
import faker
import random
from airflow.operators.python import PythonOperator

fake = faker.Faker()

""" start_date лучше не использовать datetime.now()  """

default_args = {

    "owner": "amir",
    "start_date": datetime(2025, 4, 6),
    "retry": 1,     
    "retry_delay": timedelta(minutes=5)
}

""" catchup определяет должен ли планировщик выполнять пропущеные задачи в период от start_date и текущ датой"""
dag = DAG(
    'postgres_data_generator',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    cetchup=False
)

def create_database(conn):
    conn = psycopg2.connect(
        host='postgres',
        database = 'airflow',
        user='airflow',
        password='airflow',
        port=5432
    )
    conn.autocommit = True # каждый SQL запрос автоматически подтверждается т.е не надо использовать conn.commit() 
    cursor = conn.cursor() # объект которому направляются запросы через execute

    cursor.execute("SELECT 1 FROM pg_database where datename = 'analytics_team'")

    resp = cursor.fetchone()
    if not resp:
        cursor.execute("CREATE DATABASE analitics_team")
    cursor.close()
    conn.autocommit = False


def create_table():
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()
    # shema - именованная группа объекты базы данных 
    cursor.execute('CREATE SCHEMA IF NOT EXISTS stage')
    cursor.excute(
            """
        CREATE TABLE IF NOT EXISTS stage.transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255),
            timestamp TIMESTAMP,
            amount DECIMAL,
            currency VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            merchant_name VARCHAR(255),
            payment_method VARCHAR(255),
            ip_address VARCHAR(255),
            voucher_code VARCHAR(255),
            affiliateId VARCHAR(255)
        )
        """)
    cursor.close()
    conn.commit()



def generate_transaction():
    user = fake.simple_profile()
    return {
        "transactionId": fake.uuid4(),
        "userId": user['username'],
        "timestamp":  datetime.now(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(['USD', 'GBP']),
        'city': fake.city(),
        "country": fake.country(),
        "merchantName": fake.company(),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer']),
        "ipAddress": fake.ipv4(),
        "voucherCode": random.choice(['', 'DISCOUNT10', '']),
        'affiliateId': fake.uuid4()
    }



def insert_data(connect, numrecords):
    connect = psycopg2.connect(
        host='postgres',
        port=5432,
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = connect.cursor()
    for _ in range(numrecords):
        transactions = generate_transaction()
        try:
            cursor.execute(
                """INSERT INTO stage.transactions
                    VALUES (%s, %s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """,
                (transactions["transactionId"],
                transactions["userId"],
                transactions["timestamp"],
                transactions["amount"],
                transactions["currency"],
                transactions["city"],
                transactions["country"],
                transactions["merchantName"],
                transactions["ipAddress"],
                transactions["voucherCode"],
                transactions["transactionId"],
                transactions["affiliateId"],    
                )
            )



        except Exception as e:
            print(f"ошибка вставки: {e}")
            connect.rollback() # используется в транзакциях базы данных в случае ошибки 
        else:
            connect.commit()


        cursor.close()
        connect.close()

""""""


