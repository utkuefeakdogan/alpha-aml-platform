"""Trigger GenAI SAR generation for newly flagged account groups."""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _event_log import log_dag_failure

default_args = {
    "owner": "alpha-aml",
    "retries": 1,
    "depends_on_past": False,
    "on_failure_callback": log_dag_failure,
}


def run_sar_generator(**context) -> int:
    # "once" -> run a single SAR batch and exit. Without it the module enters
    # the long-lived worker loop (used by the sar-worker container) and would
    # block the Airflow executor slot forever.
    result = subprocess.run(
        ["python", "-m", "src.ai_models.sar_generator", "once"],
        cwd="/opt/airflow",
        env={**os.environ},
        capture_output=True,
        text=True,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"SAR generator failed: {result.stderr}")
    return 0


with DAG(
    dag_id="trigger_sar",
    default_args=default_args,
    description="Generate SAR reports for flagged accounts",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aml", "genai"],
) as dag:
    PythonOperator(
        task_id="generate_sar_reports",
        python_callable=run_sar_generator,
        # Safety net: never let this task hang and starve the single executor slot.
        execution_timeout=timedelta(minutes=10),
    )
