from airflow import DAG

from airflow.models import Variable

import requests
import json
from datetime import datetime

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import time

from airflow.operators.python import PythonOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def create_table_if_not_exists():
    bq = BigQueryHook(use_legacy_sql=False, gcp_conn_id="google_cloud")
    bq.create_empty_table(
        project_id=Variable.get('gcp_project_id'),
        dataset_id = "raw",
        table_id = "googleplacesapi",
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
    WITH meta_osm_ids AS (
        SELECT
            gmp.meta_osm_id,
            MAX(gmp.created_at) AS created_at
        FROM
            `{Variable.get('gcp_project_id')}.raw.googleplacesapi` AS gmp
        WHERE
            gmp.created_at >= '{datetime.today().replace(day=1).strftime('%Y-%m-%d')}'
        GROUP BY
            gmp.meta_osm_id
        ORDER BY
            created_at DESC
    ),
    meta_osm_ids_count AS (
        SELECT
            GREATEST(0, 5000 - COUNT(*)) as result
        FROM
            meta_osm_ids
    ),
    restaurants_to_process AS (
        SELECT
            osm_name,
            geo_osm,
            alimconfiance_name,
            geo_alimconfiance,
            meta_osm_id,
            adresse_2_ua,
            code_postal,
            com_name,
            RANK() OVER(ORDER BY meta_osm_id ASC) AS ranking
        FROM
            `{Variable.get('gcp_project_id')}.mart.restaurants_final_matching`
        WHERE
            meta_osm_id NOT IN (SELECT meta_osm_id FROM meta_osm_ids)
        ORDER BY
            ranking ASC
    )
    SELECT
        *
    FROM
        restaurants_to_process
    WHERE
        ranking <= (SELECT result FROM meta_osm_ids_count)
    """
    results = bq.get_pandas_df(query)

    return results

def get_google_places_informations(restaurants):
    url = 'https://places.googleapis.com/v1/places:searchText'
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": Variable.get("google_maps_platform_api_key"),
        "X-Goog-FieldMask": "places.accessibilityOptions,places.addressComponents,places.adrFormatAddress,places.businessStatus,places.displayName,places.formattedAddress,places.googleMapsUri,places.iconBackgroundColor,places.iconMaskBaseUri,places.location,places.photos,places.plusCode,places.primaryType,places.primaryTypeDisplayName,places.shortFormattedAddress,places.subDestinations,places.types,places.utcOffsetMinutes,places.viewport,places.currentOpeningHours,places.currentSecondaryOpeningHours,places.internationalPhoneNumber,places.nationalPhoneNumber,places.priceLevel,places.rating,places.regularOpeningHours,places.regularSecondaryOpeningHours,places.userRatingCount,places.websiteUri"
    }

    gcs_hook = GCSHook(gcp_conn_id = "google_cloud")
    bigquery_hook = BigQueryHook(gcp_conn_id = "google_cloud")

    restaurants_to_bigquery = []

    for index, row in restaurants.iterrows():
        osm_name = row["osm_name"]
        geo_osm = row["geo_osm"]
        alimconfiance_name = row["alimconfiance_name"]
        geo_alimconfiance = row["geo_alimconfiance"]
        meta_osm_id = row["meta_osm_id"]
        adresse_2_ua = row["adresse_2_ua"]
        code_postal = row["code_postal"]
        com_name = row["com_name"]

        data = {
            "textQuery": f"{osm_name} {adresse_2_ua} {code_postal} {com_name}"
        }

        response = requests.post(url, headers = headers, data = json.dumps(data))

        if response.status_code == 200:

            response_data = response.json()
            
            restaurants_to_bigquery.append({
                "meta_osm_id": meta_osm_id,
                "response": json.dumps(response_data),
                "created_at": datetime.utcnow().isoformat()
            })

            try:
                gcs_hook.upload(
                    bucket_name = Variable.get("gcs_bucket"),
                    object_name = f"bronze/googleplacesapi/textsearch/{meta_osm_id}.json",
                    data = json.dumps(response_data),
                    mime_type = "application/json"
                )
            except Exception as e:
                print(f"Erreur pour le restaurant {osm_id}: {str(e)}")

        if len(restaurants_to_bigquery) == 10 or index == (len(restaurants) - 1):
            try:
                bigquery_hook.insert_all(
                    project_id = Variable.get('gcp_project_id'),
                    dataset_id = "raw",
                    table_id = "googleplacesapi",
                    rows = restaurants_to_bigquery
                )
            except Exception as e:
                print(f"Erreur lors de l'insertion dans BigQuery: {str(e)}")

            restaurants_to_bigquery = []

        time.sleep(0.1)

    return True

def get_google_details():
    create_table_if_not_exists()
    restaurants = get_restaurants_to_process()
    return get_google_places_informations(restaurants)

with DAG(
    "google_maps_platform",
    default_args = {"depends_on_past": True},
    description = "Get Google Details",
    schedule_interval = None,
    start_date = datetime(2024, 11, 7),
    catchup = False
) as dag:

    get_google_details_task = PythonOperator(
        task_id='get_google_details',
        python_callable=get_google_details,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id = "trigger_dbt",
        trigger_dag_id = "dbt",
        dag = dag
    )
    
    get_google_details_task >> trigger_dbt