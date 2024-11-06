#!/usr/bin/env bash
cd dbt_suretbon && dbt deps && cd ../
airflow scheduler