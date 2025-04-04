from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

def aggregate_and_export(**kwargs):
    # BigQuery: агрегируем общее
    bq = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)

    # Общие метрики
    sql_summary = """
        SELECT
          CURRENT_DATE() AS snapshot_date,
          COUNT(*) AS total_customers,
          AVG(age) AS avg_age
        FROM `de-pet-project-first.de_pet_project_dataset.customers`
        WHERE created_at IS NOT NULL
    """
    df_summary = bq.get_pandas_df(sql_summary)

    # Топ профессий (job as profession)
    sql_top_professions = """
        SELECT
          CURRENT_DATE() AS snapshot_date,
          job AS profession,
          COUNT(*) AS profession_count
        FROM `de-pet-project-first.de_pet_project_dataset.customers`
        WHERE job IS NOT NULL
        GROUP BY job
        ORDER BY profession_count DESC
        LIMIT 10
    """
    df_professions = bq.get_pandas_df(sql_top_professions)

    # PostgreSQL экспорт
    pg = PostgresHook(postgres_conn_id="postgres_default")

    # Очистка данных за сегодня перед вставкой
    pg.run("DELETE FROM customer_aggregates WHERE snapshot_date = CURRENT_DATE")
    pg.run("DELETE FROM top_professions WHERE snapshot_date = CURRENT_DATE")

    pg.insert_rows(
        table="customer_aggregates",
        rows=df_summary.values.tolist(),
        target_fields=df_summary.columns.tolist(),
        commit_every=1
    )

    pg.insert_rows(
        table="top_professions",
        rows=df_professions.values.tolist(),
        target_fields=df_professions.columns.tolist(),
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

    pg.run("""
        CREATE TABLE IF NOT EXISTS top_professions (
            snapshot_date DATE,
            profession TEXT,
            profession_count INTEGER
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
