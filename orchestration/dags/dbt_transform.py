"""Run dbt models to build gold AML analytics layer."""

from __future__ import annotations

import os
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _event_log import log_dag_failure

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
# dbt lives in an isolated venv baked into the image (see Dockerfile.airflow).
DBT_BIN = os.getenv("DBT_BIN", "/opt/dbt-venv/bin/dbt")

default_args = {
    "owner": "alpha-aml",
    "retries": 1,
    "depends_on_past": False,
    "on_failure_callback": log_dag_failure,
}

with DAG(
    dag_id="dbt_transform",
    default_args=default_args,
    description="dbt run for staging and gold AML models",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aml", "dbt", "lineage"],
) as dag:
    BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} run --profiles-dir . && "
            f"{DBT_BIN} test --profiles-dir ."
        ),
        append_env=True,
        env={
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "user"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "password"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "datadb"),
        },
    )
