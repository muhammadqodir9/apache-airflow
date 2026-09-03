from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['muhammadqodirs300@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def hello_operator(**kwargs):
    return 'Hello World!'

def echo_operator(**kwargs):
    return 'Echo World!'
def echo_operator_2(**kwargs):
    return 'Echo World!'




with DAG(
    dag_id="third_dag",
    default_args=default_args,
    description="This is a first dag",
    schedule='@hourly',
    start_date=datetime(2026,9,3),
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id="task1",
        python_callable=hello_operator,
)

    task2 = PythonOperator(
        task_id="task2",
        python_callable=echo_operator,
)
    task3 = PythonOperator(
        task_id="task3",
        python_callable=echo_operator_2,
)

    task4 = BashOperator(
        task_id="task4",
        bash_command="echo Hello World!",
)
    task1>> task2>> task3>> task4