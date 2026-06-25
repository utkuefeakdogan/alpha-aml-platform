"""Delete raw transactions older than retention window."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

RETENTION_HOURS = int(os.getenv("RAW_RETENTION_HOURS", "24"))
POSTGRES_CONN_ID = "postgres_aml"


def cleanup_raw(**context) -> int:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    cutoff = datetime.utcnow() - timedelta(hours=RETENTION_HOURS)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM aml.raw_transactions WHERE ingested_at < %s",
        (cutoff,),
    )
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"Deleted {deleted} raw rows older than {RETENTION_HOURS}h")
    return deleted


default_args = {
    "owner": "alpha-aml",
    "retries": 1,
    "depends_on_past": False,
}

with DAG(
    dag_id="cleanup_raw_data",
    default_args=default_args,
    description="Remove raw transactions older than 24 hours",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aml", "lifecycle"],
) as dag:
    PythonOperator(
        task_id="delete_stale_raw",
        python_callable=cleanup_raw,
    )
