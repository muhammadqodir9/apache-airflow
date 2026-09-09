from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd


def success_email():
    print("success email")
def fail_email():
    print("fail email")



def hello_operator(**kwargs):
    print("hello operator")


def load_excel():
    df = pd.read_excel(
        "/opt/airflow/data/airflow.xlsx"
    )
    print(df)


with DAG(
    dag_id="fifth_dag",
    start_date=datetime(2026, 9, 8),
    schedule="@hourly",
    catchup=False,
    on_success_callback=success_email,
    on_failure_callback=fail_email,
) as dag:

    task1 = PythonOperator(
        task_id="task1",
        python_callable=hello_operator
    )

    task2 = BashOperator(
        task_id="task2",
        bash_command="echo hello world"
    )

    extract_data = PythonOperator(
        task_id="extract_excel",
        python_callable=load_excel
    )

    task1 >> task2 >> extract_data