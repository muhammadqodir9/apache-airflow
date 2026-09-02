from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def say_hello():
    print("Hello from my construction pipeline!")


with DAG(
    dag_id="construction_pipeline",
    start_date=datetime(2026, 9, 2),
    description="This is a construction pipeline DAG",
    schedule=None,
    catchup=False,
) as dag:

    say_hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )