from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from datetime import datetime


# -------------------------
# EXTRACT FROM SQL SERVER
# -------------------------
def extract_from_sql_server():

    hook = MsSqlHook(
        mssql_conn_id="sql_server"
    )

    df = hook.get_pandas_df(
        sql="""
            SELECT *
            FROM orders;
        """
    )

    print("Data extracted from SQL Server:")
    print(df)

    return df


# -------------------------
# SAVE AS CSV
# -------------------------
def save_to_csv():

    hook = MsSqlHook(
        mssql_conn_id="sql_server"
    )

    df = hook.get_pandas_df(
        sql="""
            SELECT *
            FROM orders;
        """
    )

    file_path = "/opt/airflow/data/orders.csv"

    df.to_csv(
        file_path,
        index=False
    )

    print(f"CSV file created: {file_path}")
    print(f"Rows written: {len(df)}")


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="sql_server_to_csv",

    start_date=datetime(
        2026,
        9,
        17
    ),

    schedule="@daily",

    catchup=False,

) as dag:

    extract_and_save = PythonOperator(
        task_id="extract_and_save",
        python_callable=save_to_csv,
    )