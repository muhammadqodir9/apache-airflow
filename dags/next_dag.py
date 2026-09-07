from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


def hello_world():
    return 'Hello World!'



with DAG(
    dag_id = 'next_dag',
    description = 'next dag has been created',
    start_date=datetime(2026,9,5),
    schedule='@hourly',
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='task1',
        python_callable=hello_world,
    )
    task2 = PythonOperator(
        task_id='task2',
        python_callable=hello_world,

    )
    task3 = PythonOperator(
        task_id='task3',
        python_callable=hello_world,

    )

    task1 >> task2>>task3

