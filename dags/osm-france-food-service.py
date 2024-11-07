from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime
import requests
import os

def download_and_upload_to_gcs(**context):
    date_str = context['execution_date'].strftime('%Y-%m-%d')
    filename = f"{date_str}.parquet"
    gcs_destination = f"bronze/osm-france-food-service/{filename}"
    url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/osm-france-food-service/exports/parquet?lang=fr&timezone=Europe%2FBerlin"
    response = requests.get(url)
    temp_path = f"/tmp/osm-france-food-service-{filename}"
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
    "osm_food_service_etl",
    default_args = {"depends_on_past": True},
    description = "ETL for OSM Food Service data",
    schedule_interval = "0 2 * * 1",
    start_date = datetime(2024, 11, 4),
    catchup = False
) as dag:

    download_task = PythonOperator(
        task_id='download_and_upload_to_gcs',
        python_callable=download_and_upload_to_gcs,
        provide_context=True,
    )

    load_to_temp = GCSToBigQueryOperator(
        task_id='load_to_temp_table',
        bucket=Variable.get('gcs_bucket'),
        source_objects=["{{ task_instance.xcom_pull(task_ids='download_and_upload_to_gcs', key='filename') }}"],
        destination_project_dataset_table=f"{Variable.get('gcp_project_id')}.raw.temp_osm_food_service",
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud',
    )

    load_to_bq = BigQueryExecuteQueryOperator(
        task_id='load_to_bigquery',
        sql=f"""
        CREATE TABLE IF NOT EXISTS `{Variable.get('gcp_project_id')}.raw.osm-france-food-service`
        LIKE `{Variable.get('gcp_project_id')}.raw.temp_osm_food_service`;
        
        MERGE `{Variable.get('gcp_project_id')}.raw.osm-france-food-service` T
        USING (
            SELECT *
            FROM `{Variable.get('gcp_project_id')}.raw.temp_osm_food_service`
        ) S
        ON T.meta_osm_id = S.meta_osm_id
        WHEN MATCHED THEN
            UPDATE SET 
                T.name = S.name,
                T.type = S.type,
                T.operator = S.operator,
                T.brand = S.brand,
                T.cuisine = S.cuisine,
                T.vegetarian = S.vegetarian,
                T.vegan = S.vegan,
                T.opening_hours = S.opening_hours,
                T.wheelchair = S.wheelchair,
                T.delivery = S.delivery,
                T.takeaway = S.takeaway,
                T.drive_through = S.drive_through,
                T.internet_access = S.internet_access,
                T.capacity = S.capacity,
                T.stars = S.stars,
                T.smoking = S.smoking,
                T.wikidata = S.wikidata,
                T.brand_wikidata = S.brand_wikidata,
                T.siret = S.siret,
                T.phone = S.phone,
                T.website = S.website,
                T.facebook = S.facebook,
                T.meta_name_com = S.meta_name_com,
                T.meta_code_com = S.meta_code_com,
                T.meta_name_dep = S.meta_name_dep,
                T.meta_code_dep = S.meta_code_dep,
                T.meta_name_reg = S.meta_name_reg,
                T.meta_code_reg = S.meta_code_reg,
                T.meta_geo_point = S.meta_geo_point,
                T.meta_osm_url = S.meta_osm_url,
                T.meta_first_update = S.meta_first_update,
                T.meta_last_update = S.meta_last_update,
                T.meta_versions_number = S.meta_versions_number,
                T.meta_users_number = S.meta_users_number
        WHEN NOT MATCHED THEN
            INSERT (
                name, type, operator, brand, cuisine, vegetarian, vegan, 
                opening_hours, wheelchair, delivery, takeaway, drive_through, 
                internet_access, capacity, stars, smoking, wikidata, 
                brand_wikidata, siret, phone, website, facebook, 
                meta_name_com, meta_code_com, meta_name_dep, meta_code_dep, 
                meta_name_reg, meta_code_reg, meta_geo_point, meta_osm_id, 
                meta_osm_url, meta_first_update, meta_last_update, 
                meta_versions_number, meta_users_number
            )
            VALUES (
                S.name, S.type, S.operator, S.brand, S.cuisine, S.vegetarian, 
                S.vegan, S.opening_hours, S.wheelchair, S.delivery, S.takeaway, 
                S.drive_through, S.internet_access, S.capacity, S.stars, 
                S.smoking, S.wikidata, S.brand_wikidata, S.siret, S.phone, 
                S.website, S.facebook, S.meta_name_com, S.meta_code_com, 
                S.meta_name_dep, S.meta_code_dep, S.meta_name_reg, S.meta_code_reg, 
                S.meta_geo_point, S.meta_osm_id, S.meta_osm_url, S.meta_first_update, 
                S.meta_last_update, S.meta_versions_number, S.meta_users_number
            )
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud',
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='dbt',
        dag=dag
    )

    download_task >> load_to_temp >> load_to_bq >> trigger_dbt
