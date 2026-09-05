from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.bash import BashOperator
def hello():
    print("Hello from my first Airflow DAG!")
def hello2():
    print("Hello second Airflow DAG!")
def get_name():
    return "Tom"

with DAG(
    dag_id="first_dag1",
    description="This is a first dag",
    start_date=datetime(2026, 9, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )

    task2 = PythonOperator(
        task_id="hello_task2",
        python_callable=hello2,
    )
    task3=BashOperator(
        task_id="bash_task",
        bash_command="echo hello world",

    )

    task3<<task2<<task


