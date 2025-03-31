from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime

log = LoggingMixin().log

def check_gcs_files_exist(**context):
    execution_date = context['ds']
    bucket_name = "de-pet-project-data"
    prefix = f"raw/{execution_date}/"

    hook = GCSHook(gcp_conn_id="google_cloud_default")
    all_files = hook.list(bucket_name=bucket_name, prefix=prefix)
    matching_files = [f for f in all_files if f.endswith(".csv") and f.startswith(prefix + "data_")]

    if not matching_files:
        raise AirflowSkipException(f"No files found in {prefix} to process.")
    else:
        log.info(f"Found {len(matching_files)} files to load: {matching_files}")
        context['ti'].xcom_push(key='matching_files', value=matching_files)

def decide_branch(**kwargs):
    hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    sql = """
            SELECT COUNT(*) as duplicate_count FROM (
                SELECT email, COUNT(*) as cnt
                FROM `de-pet-project-first.de_pet_project_dataset.customers`
                GROUP BY email
                HAVING cnt > 1
            )
        """
    records = hook.get_pandas_df(sql).to_dict(orient='records')
    count = records[0]['duplicate_count'] if records else 0

    if count > 0:
        return 'trigger_clean_dag'
    else:
        return 'skip_cleanup'

with DAG(
    dag_id="gcs_to_bigquery_with_validation",
    start_date=datetime(2024, 3, 27),
    schedule_interval=None,
    catchup=False
) as dag:

    check_files = PythonOperator(
        task_id="check_gcs_files",
        python_callable=check_gcs_files_exist,
        provide_context=True
    )

    load_data_to_bq = GCSToBigQueryOperator(
        task_id="load_data_to_bq",
        bucket="de-pet-project-data",
        source_objects=[
            "raw/2025-03-25/data_*.csv",
            "raw/2025-03-26/data_*.csv",
            "raw/2025-03-27/data_*.csv",
            "raw/2025-03-28/data_*.csv",
            "raw/2025-03-29/data_*.csv"
        ],
        destination_project_dataset_table="de-pet-project-first.de_pet_project_dataset.customers",
        schema_fields=[
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
            {"name": "address", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "job", "type": "STRING", "mode": "NULLABLE"},
            {"name": "manager", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="google_cloud_default"
    )

    count_duplicates = BigQueryInsertJobOperator(
        task_id="count_duplicates",
        configuration={
            "query": {
                "query": """
                    SELECT COUNT(*) as duplicate_count FROM (
                        SELECT email, COUNT(*) as cnt
                        FROM `de-pet-project-first.de_pet_project_dataset.customers`
                        GROUP BY email
                        HAVING cnt > 1
                    )
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id="google_cloud_default"
    )

    branch = BranchPythonOperator(
        task_id="branch_on_duplicates",
        python_callable=decide_branch,
        provide_context=True
    )

    trigger_clean_dag = TriggerDagRunOperator(
        task_id="trigger_clean_dag",
        trigger_dag_id="remove_duplicates_dag",
    )

    skip_cleanup = DummyOperator(task_id="skip_cleanup")

    check_files >> load_data_to_bq >> count_duplicates >> branch
    branch >> trigger_clean_dag
    branch >> skip_cleanup
