from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime


default_args = {
    'start_date': datetime(2024, 3, 27),
    'catchup': False
}

with DAG(
    dag_id="remove_duplicates_dag",
    default_args=default_args,
    schedule_interval=None,
    description="Remove duplicate customers by email in BigQuery",
) as dag:

    remove_duplicates = BigQueryInsertJobOperator(
        task_id="remove_duplicates",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `de-pet-project-first.de_pet_project_dataset.customers` AS
                    SELECT * EXCEPT(rn)
                    FROM (
                      SELECT *,
                             ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) AS rn
                      FROM `de-pet-project-first.de_pet_project_dataset.customers`
                    )
                    WHERE rn = 1
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default"
    )
