"""Daily Gold sync: Postgres gold.* → Parquet → GCS → BigQuery.

Requires env (see .env.example):
  GCP_PROJECT, GCS_BUCKET, BQ_DATASET
  GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/secrets/gcp-sa.json
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from _event_log import log_dag_failure

# Airflow mounts ./src at /opt/airflow/src — ensure importable.
if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "alpha-aml",
    "retries": 1,
    "depends_on_past": False,
    "on_failure_callback": log_dag_failure,
}


def _run_export(**_context) -> dict:
    logging.basicConfig(level=logging.INFO)
    # Fail fast with a readable message when the GCP bridge is not wired yet.
    required = ("GCP_PROJECT", "GCS_BUCKET", "BQ_DATASET")
    missing = [k for k in required if not os.getenv(k)]
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if missing:
        raise RuntimeError(
            f"export_to_bigquery skipped config: missing {missing}. "
            "Set them in .env and recreate the airflow service."
        )
    if not creds or not os.path.isfile(creds):
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS is missing or not a file inside the "
            "container (expected /opt/airflow/secrets/gcp-sa.json). "
            "Place the SA JSON on the host under ./secrets/gcp-sa.json (gitignored)."
        )

    from src.common.bq_export import run_export

    return run_export()


with DAG(
    dag_id="export_to_bigquery",
    default_args=default_args,
    description="Sync Postgres Gold tables to BigQuery via GCS Parquet",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aml", "bigquery", "warehouse"],
) as dag:
    PythonOperator(
        task_id="gold_to_bigquery",
        python_callable=_run_export,
    )
