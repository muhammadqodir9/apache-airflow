from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


def extact_data():
    file_path = "/opt/airflow/data/airflow.xlsx"
    df = pd.read_excel(file_path)
    return df


with DAG(
    dag_id="test_dag",
    start_date=datetime(2021, 1, 1),
    schedule = "@hourly",
    catchup = False,

) as dag:

    extract_data =PythonOperator(
        task_id="extract_data",
        python_callable=extact_data,

    )



