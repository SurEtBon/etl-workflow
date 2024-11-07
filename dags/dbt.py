import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = os.getenv("DBT_DIR")

with DAG(
    "dbt",
    default_args = {"depends_on_past": True},
    start_date = datetime(2024, 11, 7),
    schedule_interval = None,
    catchup = False
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR}",
    )

    dbt_run >> dbt_test
