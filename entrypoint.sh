#!/usr/bin/env bash
airflow db upgrade
airflow users create -r Admin -u $AIRFLOW_ADMIN_USERNAME -p $AIRFLOW_ADMIN_PASSWORD -e $AIRFLOW_ADMIN_EMAIL -f $AIRFLOW_ADMIN_FIRST_NAME -l $AIRFLOW_ADMIN_LAST_NAME
airflow webserver