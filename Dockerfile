FROM python:3.9.20-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED 1

ENV AIRFLOW_HOME=/app/airflow

ENV GCP_PROJECT_ID=$GCP_PROJECT_ID
ENV GCP_LOCATION=$GCP_LOCATION

ENV DBT_DIR=$AIRFLOW_HOME/dbt_suretbon
ENV DBT_TARGET_DIR=$DBT_DIR/target
ENV DBT_PROFILES_DIR=$DBT_DIR
ENV DBT_VERSION=1.8.8

WORKDIR $AIRFLOW_HOME

COPY ./scheduler_entrypoint.sh ./scheduler_entrypoint.sh
RUN chmod +x ./scheduler_entrypoint.sh

COPY ./entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main