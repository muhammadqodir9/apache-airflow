from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


def extract_data():
    file_path = "/opt/airflow/data/airflow.xlsx"

    df = pd.read_excel(file_path)

    print("Extracted data:")
    print(df)

    return df.to_json()


def transform_data(**kwargs):
    ti = kwargs["ti"]

    data = ti.xcom_pull(
        task_ids="extract_data"
    )

    df = pd.read_json(data)

    # Example transformation
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    print("Transformed data:")
    print(df)

    return df.to_json()


def load_data(**kwargs):
    ti = kwargs["ti"]

    data = ti.xcom_pull(
        task_ids="transform_data"
    )

    df = pd.read_json(data)

    print("Data that will be loaded into Supabase:")
    print(df)

    # Supabase/PostgreSQL insertion will go here later


with DAG(
    dag_id="excel_to_supabase",
    start_date=datetime(2026, 9, 9),
    schedule="@daily",
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract >> transform >> load