from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

def aggregate_and_export(**kwargs):
    # BigQuery: агрегируем данные
    bq = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    sql = """
        SELECT
          CURRENT_DATE() AS snapshot_date,
          COUNT(*) AS total_customers,
          AVG(age) AS avg_age
        FROM `de-pet-project-first.de_pet_project_dataset.customers`
        WHERE created_at IS NOT NULL
    """
    df = bq.get_pandas_df(sql)

    # PostgreSQL: экспорт агрегатов
    pg = PostgresHook(postgres_conn_id="postgres_default")
    pg.insert_rows(
        table="customer_aggregates",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        commit_every=1
    )

def create_postgres_table():
    pg = PostgresHook(postgres_conn_id="postgres_default")
    pg.run("""
        CREATE TABLE IF NOT EXISTS customer_aggregates (
            snapshot_date DATE,
            total_customers INTEGER,
            avg_age FLOAT
        )
    """)

with DAG(
    dag_id="aggregate_to_postgres_dag",
    start_date=datetime(2024, 3, 28),
    schedule_interval="@daily",
    catchup=False
) as dag:

    init_table = PythonOperator(
        task_id="create_pg_table",
        python_callable=create_postgres_table
    )

    export_aggregates = PythonOperator(
        task_id="aggregate_and_export",
        python_callable=aggregate_and_export
    )

    init_table >> export_aggregates
