from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from io import StringIO

import pandas as pd


# -------------------------
# EXTRACT
# -------------------------
def extract_data():
    file_path = "/opt/airflow/data/airflow.xlsx"

    df = pd.read_excel(file_path)

    print("Extracted data:")
    print(df)

    return df.to_json()


# -------------------------
# TRANSFORM
# -------------------------
def transform_data(**kwargs):
    ti = kwargs["ti"]

    data = ti.xcom_pull(
        task_ids="extract_data"
    )

    df = pd.read_json(StringIO(data))

    # Standardize column names
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
    )

    print("Transformed data:")
    print(df)

    return df.to_json()


# -------------------------
# LOAD
# -------------------------
def load_data(**kwargs):
    ti = kwargs["ti"]

    data = ti.xcom_pull(
        task_ids="transform_data"
    )

    df = pd.read_json(StringIO(data))

    print("Data that will be loaded into Supabase:")
    print(df)

    hook = PostgresHook(
        postgres_conn_id="supabase_id"
    )

    connection = hook.get_conn()
    cursor = connection.cursor()

    for _, row in df.iterrows():

        cursor.execute(
            """
            INSERT INTO orders
            (order_id, product, quantity, price, country)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (order_id)
            DO UPDATE SET
                product = EXCLUDED.product,
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                country = EXCLUDED.country;
            """,
            (
                row["order_id"],
                row["product"],
                row["quantity"],
                row["price"],
                row["country"],
            )
        )

    connection.commit()

    cursor.close()
    connection.close()

    print(f"Loaded {len(df)} rows into Supabase.")


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="excel_to_supabase",
    start_date=datetime(2026, 9, 9),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
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