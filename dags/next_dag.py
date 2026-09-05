from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id = 'next_dag',
    description = 'next dag has been created',
    start_date=datetime(2026,9,5),
    schedule='@hourly',
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='task1',
    )
    task2 = PythonOperator(
        task_id='task2',
    )
    task3 = PythonOperator(
        task_id='task3',
    )

    task1 >> task2>>task3

