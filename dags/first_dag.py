from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello():
    print("Hello from my first Airflow DAG!")


with DAG(
    dag_id="first_dag",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )


