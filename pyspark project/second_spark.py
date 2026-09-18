from airflow import DAG
from airflow.operator.PythonOperator
from datetime import datetime





with DAG(
    dag_id="second_spark",
    start_date=datetime(2021, 1, 1),
    schedule="@hourly",
    catchup=False,

)


