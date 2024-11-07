from airflow import DAG

from airflow.models import Variable

import urllib.parse

import requests
import json
from datetime import datetime

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import time

from airflow.operators.python import PythonOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def create_table_if_not_exists(table_id):
    bq = BigQueryHook(use_legacy_sql=False, gcp_conn_id="google_cloud")
    bq.create_empty_table(
        project_id=Variable.get('gcp_project_id'),
        dataset_id = "raw",
        table_id = table_id,
        schema_fields = [
            {
                "name": "meta_osm_id",
                "type": "INTEGER",
                "mode": "REQUIRED"
            },
            {
                "name": "response",
                "type": "JSON",
                "mode": "REQUIRED"
            },
            {
                "name": "created_at",
                "type": "DATETIME",
                "mode": "REQUIRED"
            }
        ],
        exists_ok = True
    )
    

def get_restaurants_to_process():
    bq = BigQueryHook(use_legacy_sql=False, gcp_conn_id="google_cloud")
    query = f"""
    WITH tals_meta_osm_ids AS (
        SELECT
            tals.meta_osm_id,
            MAX(tals.created_at) AS created_at
        FROM
            `{Variable.get('gcp_project_id')}.raw.tripadvisor-location-search` AS tals
        WHERE
            tals.created_at >= '{datetime.today().replace(day=1).strftime('%Y-%m-%d')}'
        GROUP BY
            tals.meta_osm_id
        ORDER BY
            created_at DESC
    ),
    tals_meta_osm_ids_count AS (
        SELECT
            GREATEST(0, 2375 - COUNT(*)) as result
        FROM
            tals_meta_osm_ids
    ), tald_meta_osm_ids AS (
        SELECT
            tald.meta_osm_id,
            MAX(tald.created_at) AS created_at
        FROM
            `{Variable.get('gcp_project_id')}.raw.tripadvisor-location-search` AS tald
        WHERE
            tald.created_at >= '{datetime.today().replace(day=1).strftime('%Y-%m-%d')}'
        GROUP BY
            tald.meta_osm_id
        ORDER BY
            created_at DESC
    ),
    tald_meta_osm_ids_count AS (
        SELECT
            GREATEST(0, 2375 - COUNT(*)) as result
        FROM
            tald_meta_osm_ids
    ),
    restaurants_to_process AS (
        SELECT
            osm_name,
            ROUND(ST_Y(geo_osm), 4) as latitude,
            ROUND(ST_X(geo_osm), 4) as longitude,
            meta_osm_id,
            RANK() OVER(ORDER BY meta_osm_id ASC) AS ranking
        FROM
            `{Variable.get('gcp_project_id')}.mart.restaurants_final_matching`
        WHERE
            meta_osm_id NOT IN (SELECT meta_osm_id FROM tals_meta_osm_ids)
        ORDER BY
            ranking ASC
    )
    SELECT
        *
    FROM
        restaurants_to_process
    WHERE
        ranking <= LEAST((SELECT result FROM tals_meta_osm_ids_count), (SELECT result FROM tald_meta_osm_ids_count))
    """
    results = bq.get_pandas_df(query)

    return results

def get_tripadvisor_informations(restaurants):
    location_search_url = f"https://api.content.tripadvisor.com/api/v1/location/search?key={Variable.get('tripadvisor_api_key')}"
    headers = {
        "accept": "application/json"
    }

    gcs_hook = GCSHook(gcp_conn_id = "google_cloud")
    bigquery_hook = BigQueryHook(gcp_conn_id = "google_cloud")

    restaurants_location_search_to_bigquery = []

    restaurants_location_details_to_bigquery = []

    for index, row in restaurants.iterrows():
        osm_name = row["osm_name"]
        latitude = row["latitude"]
        longitude = row["longitude"]
        meta_osm_id = row["meta_osm_id"]

        location_search_url_with_query = f"{location_search_url}&searchQuery={urllib.parse.quote(osm_name)}&latLong={latitude}%2C{longitude}&language=fr"

        location_search_response = requests.get(location_search_url_with_query, headers = headers)

        print(location_search_response.json())

        if location_search_response.status_code == 200:

            location_search_response_data = location_search_response.json()
            
            restaurants_location_search_to_bigquery.append({
                "meta_osm_id": meta_osm_id,
                "response": json.dumps(location_search_response_data),
                "created_at": datetime.utcnow().isoformat()
            })

            try:
                gcs_hook.upload(
                    bucket_name = Variable.get("gcs_bucket"),
                    object_name = f"bronze/tripadvisor/location-search/{meta_osm_id}.json",
                    data = json.dumps(location_search_response_data),
                    mime_type = "application/json"
                )
            except Exception as e:
                print(f"Erreur location_search pour le restaurant {meta_osm_id}: {str(e)}")

        if len(restaurants_location_search_to_bigquery) == 10 or index == (len(restaurants) - 1):
            try:
                bigquery_hook.insert_all(
                    project_id = Variable.get('gcp_project_id'),
                    dataset_id = "raw",
                    table_id = "tripadvisor-location-search",
                    rows = restaurants_location_search_to_bigquery
                )
            except Exception as e:
                print(f"Erreur location_search lors de l'insertion dans BigQuery: {str(e)}")

            restaurants_location_search_to_bigquery = []

        if 'data' in location_search_response_data and len(location_search_response_data['data']) > 0:
            first_element = location_search_response_data['data'][0]
            if 'location_id' in first_element:
                location_id = first_element['location_id']
                location_details_url = f"https://api.content.tripadvisor.com/api/v1/location/{location_id}/details?key={Variable.get('tripadvisor_api_key')}&language=fr&currency=EUR"
                location_details_response = requests.get(location_details_url, headers = headers)

                if location_details_response.status_code == 200:

                    location_details_response_data = location_details_response.json()
                    
                    restaurants_location_details_to_bigquery.append({
                        "meta_osm_id": meta_osm_id,
                        "response": json.dumps(location_details_response_data),
                        "created_at": datetime.utcnow().isoformat()
                    })

                    try:
                        gcs_hook.upload(
                            bucket_name = Variable.get("gcs_bucket"),
                            object_name = f"bronze/tripadvisor/location-details/{meta_osm_id}.json",
                            data = json.dumps(location_details_response_data),
                            mime_type = "application/json"
                        )
                    except Exception as e:
                        print(f"Erreur location_details pour le restaurant {meta_osm_id}: {str(e)}")

        if len(restaurants_location_details_to_bigquery) == 10 or index == (len(restaurants) - 1):
            try:
                bigquery_hook.insert_all(
                    project_id = Variable.get('gcp_project_id'),
                    dataset_id = "raw",
                    table_id = "tripadvisor-location-details",
                    rows = restaurants_location_details_to_bigquery
                )
            except Exception as e:
                print(f"Erreur location_details lors de l'insertion dans BigQuery: {str(e)}")

            restaurants_location_details_to_bigquery = []

        time.sleep(0.04)

    return True

def get_tripadvisor_details():
    create_table_if_not_exists("tripadvisor-location-search")
    create_table_if_not_exists("tripadvisor-location-details")
    restaurants = get_restaurants_to_process()
    return get_tripadvisor_informations(restaurants)

with DAG(
    "tripadvisor",
    default_args = {"depends_on_past": True},
    description = "Get Tripadvisor Details",
    schedule_interval = None,
    start_date = datetime(2024, 11, 7),
    catchup = False
) as dag:

    get_tripadvisor_details_task = PythonOperator(
        task_id='get_tripadvisor_details',
        python_callable=get_tripadvisor_details,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id = "trigger_dbt",
        trigger_dag_id = "dbt",
        dag = dag
    )
    
    get_tripadvisor_details_task >> trigger_dbt