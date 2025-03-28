from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


def decide_next_step(**kwargs):
    # XCom вернёт значение из BigQueryValueCheckOperator
    has_duplicates = kwargs['ti'].xcom_pull(task_ids='check_duplicates')
    if has_duplicates != 0:
        return 'trigger_clean_dag'
    else:
        return 'skip_cleanup'


default_args = {
    'start_date': datetime(2024, 3, 27),
    'catchup': False
}

with DAG(
    dag_id="gcs_to_bigquery_dag",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    # Загрузка из GCS в BigQuery
    load_data_to_bq = GCSToBigQueryOperator(
        task_id="load_data_to_bq",
        bucket="de-pet-project-data",
        source_objects=["raw/{{ ds }}/data_*.csv"],
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

    # Проверка на дубликаты
    check_duplicates = BigQueryValueCheckOperator(
        task_id="check_duplicates",
        sql="""
            SELECT COUNT(*) FROM (
                SELECT email, COUNT(*) AS cnt
                FROM `de-pet-project-first.de_pet_project_dataset.customers`
                GROUP BY email
                HAVING cnt > 1
            )
        """,
        pass_value=0,
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False
    )

    # Ветвление: если дубликаты есть — запускаем другой DAG
    branch = BranchPythonOperator(
        task_id='branch_on_duplicates',
        python_callable=decide_next_step,
        provide_context=True
    )

    # Запуск дага очистки
    trigger_clean_dag = TriggerDagRunOperator(
        task_id="trigger_clean_dag",
        trigger_dag_id="remove_duplicates_dag",
    )

    # Пустышка, если всё ок
    skip_cleanup = DummyOperator(task_id="skip_cleanup")

    load_data_to_bq >> check_duplicates >> branch
    branch >> trigger_clean_dag
    branch >> skip_cleanup
