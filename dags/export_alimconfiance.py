from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_and_upload_to_gcs(**context):
    date_str = context['execution_date'].strftime('%Y-%m-%d')
    filename = f'export_alimconfiance-{date_str}.parquet'
    gcs_destination = f'bronze/{filename}'
    url = "https://dgal.opendatasoft.com/api/explore/v2.1/catalog/datasets/export_alimconfiance/exports/parquet?lang=fr&timezone=Europe%2FBerlin"
    
    response = requests.get(url)
    temp_path = f"/tmp/{filename}"
    with open(temp_path, 'wb') as f:
        f.write(response.content)
    
    gcs_hook = GCSHook(gcp_conn_id='google_cloud')
    bucket_name = Variable.get('gcs_bucket')
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=gcs_destination,
        filename=temp_path
    )
    
    os.remove(temp_path)
    context['task_instance'].xcom_push(key='filename', value=gcs_destination)

with DAG(
    'export_alimconfiance_etl',
    default_args=default_args,
    description='ETL for Alimconfiance data',
    schedule_interval='0 6 * * 1',  # Tous les lundis à 6h00
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    download_task = PythonOperator(
        task_id='download_and_upload_to_gcs',
        python_callable=download_and_upload_to_gcs,
        provide_context=True,
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=Variable.get('gcs_bucket'),
        source_objects=["{{ task_instance.xcom_pull(task_ids='download_and_upload_to_gcs', key='filename') }}"],
        destination_project_dataset_table=f"{Variable.get('gcp_project_id')}.raw.export_alimconfiance",
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud',
    )

    download_task >> load_to_bq