from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

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

    df = pd.read_json(
        StringIO(data)
    )

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

    df = pd.read_json(
        StringIO(data)
    )

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
# SUCCESS EMAIL
# -------------------------
success_email = SmtpNotifier(
    to="muhammadqodirsotvoldiyev84@gmail.com",
    from_email="muhammadqodirs300@gmail.com",
    subject="Airflow DAG succeeded",
    html_content="""
    <h2>Airflow DAG Succeeded</h2>

    <p><b>DAG:</b> {{ dag.dag_id }}</p>

    <p><b>Run ID:</b> {{ run_id }}</p>

    <p><b>Status:</b> SUCCESS</p>

    <p>
        The Excel → Transform → Supabase pipeline
        completed successfully.
    </p>
    """,
    smtp_conn_id="smtp_default",
)


# -------------------------
# FAILURE EMAIL
# -------------------------
failure_email = SmtpNotifier(
    to="muhammadqodirsotvoldiyev84@gmail.com",
    from_email="muhammadqodirs300@gmail.com",
    subject="Airflow DAG failed",
    html_content="""
    <h2>Airflow DAG Failed</h2>

    <p><b>DAG:</b> {{ dag.dag_id }}</p>

    <p><b>Run ID:</b> {{ run_id }}</p>

    <p><b>Status:</b> FAILED</p>

    <p>
        The Excel → Transform → Supabase pipeline
        failed. Check the Airflow logs for details.
    </p>
    """,
    smtp_conn_id="smtp_default",
)


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="excel_to_email_supabase",

    start_date=datetime(
        2026,
        9,
        9
    ),

    schedule="@hourly",

    catchup=False,

    max_active_runs=1,

    # Send email when the entire DAG succeeds
    on_success_callback=[
        success_email
    ],

    # Send email when the DAG fails
    on_failure_callback=[
        failure_email
    ],

) as dag:

    # Extract
    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    # Transform
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # Load
    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    # Dependencies
    extract >> transform >> load

