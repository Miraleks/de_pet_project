from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 3, 26),
    'catchup': False
}

with DAG(
    dag_id="gcs_to_bigquery_dag",
    default_args=default_args,
    schedule_interval=None,  # Запуск вручную или по Trigger
) as dag:

    # Загрузка из GCS в BigQuery
    load_data_to_bq = GCSToBigQueryOperator(
        task_id="load_data_to_bq",
        bucket="de-pet-project-data",
        source_objects=["raw/{{ ds }}/data_*.csv"],  # подставится дата выполнения DAG-а
        destination_project_dataset_table="your_project_id.your_dataset.customers",
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
        ],
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="google_cloud_default"
    )

    # Data Quality Check — на наличие дублей по email
    check_duplicates = BigQueryCheckOperator(
        task_id='check_duplicates',
        sql="""
            SELECT COUNT(*) FROM (
                SELECT email, COUNT(*) AS cnt
                FROM `your_project_id.your_dataset.customers`
                GROUP BY email
                HAVING cnt > 1
            )
        """,
        gcp_conn_id='google_cloud_default',
        use_legacy_sql=False
    )

    load_data_to_bq >> check_duplicates
