"""Train + score the AML risk models (Isolation Forest + supervised triage).

Runs `python -m src.ml.train` as a subprocess (BashOperator) so the heavy
scikit-learn import is released after each run instead of living in the
long-running SequentialExecutor scheduler process. Writes per-customer scores
to aml.ml_customer_scores and run metrics to aml.ml_model_runs.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.getenv("AIRFLOW_PROJECT_DIR", "/opt/airflow")

default_args = {
    "owner": "alpha-aml",
    "retries": 1,
    "depends_on_past": False,
}

with DAG(
    dag_id="ml_score",
    default_args=default_args,
    description="Train + score AML risk models (anomaly + triage)",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aml", "ml", "risk"],
) as dag:
    BashOperator(
        task_id="train_and_score",
        bash_command=f"cd {PROJECT_DIR} && python -m src.ml.train",
        append_env=True,
        env={
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "user"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "password"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "datadb"),
            "ML_MODEL_DIR": os.getenv("ML_MODEL_DIR", "/opt/airflow/models"),
            "PYTHONPATH": PROJECT_DIR,
        },
    )
