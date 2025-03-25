from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="upload_customer_data_to_gcs",
    schedule_interval="0 0 * * *",  # ежедневно в полночь
    start_date=datetime(2024, 3, 21),
    catchup=False,
) as dag:

    upload_files = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src="/opt/airflow/generated_data/*.csv",
        dst="raw/{{ ds }}/",
        bucket="de-pet-project-data",
        gcp_conn_id="google_cloud_default",
        mime_type="text/csv"
    )

    cleanup = BashOperator(
        task_id="cleanup_files",
        bash_command="rm -f /opt/airflow/generated_data/*.csv"
    )

    upload_files >> cleanup
