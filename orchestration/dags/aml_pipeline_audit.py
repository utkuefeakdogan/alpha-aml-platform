"""Pipeline audit: alert counts by rule and customer universe stats."""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from _event_log import log_dag_failure

POSTGRES_CONN_ID = "postgres_aml"


def _insert_metric(hook: PostgresHook, key: str, value: int, detail: str | None = None) -> None:
    hook.run(
        """
        INSERT INTO aml.pipeline_metrics (metric_key, metric_value, metric_detail)
        VALUES (%s, %s, %s)
        """,
        parameters=(key, value, detail),
    )


def pipeline_audit(**context) -> dict:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    result: dict = {}

    rows = hook.get_records(
        """
        SELECT rule_name, COUNT(*) AS cnt
        FROM aml.flagged_transactions
        WHERE flagged_at >= NOW() - INTERVAL '24 hours'
        GROUP BY rule_name
        """
    )
    for rule_name, cnt in rows:
        key = f"alerts_24h_{rule_name}"
        _insert_metric(hook, key, int(cnt), rule_name)
        result[key] = int(cnt)

    total_24h = hook.get_first(
        """
        SELECT COUNT(*) FROM aml.flagged_transactions
        WHERE flagged_at >= NOW() - INTERVAL '24 hours'
        """
    )[0]
    _insert_metric(hook, "alerts_24h_total", int(total_24h))
    result["alerts_24h_total"] = int(total_24h)

    customers, active, acquired = hook.get_first(
        """
        SELECT
          (SELECT COUNT(*) FROM aml.customers),
          (SELECT COUNT(*) FROM aml.customers WHERE customer_status = 'active'),
          (SELECT COUNT(*) FROM aml.customer_acquisition_log WHERE acquired_at >= CURRENT_DATE)
        """
    )
    _insert_metric(hook, "customers_total", int(customers))
    _insert_metric(hook, "customers_active", int(active))
    _insert_metric(hook, "customers_acquired_today", int(acquired))
    result.update(
        {
            "customers_total": int(customers),
            "customers_active": int(active),
            "customers_acquired_today": int(acquired),
        }
    )

    print(result)
    return result


default_args = {
    "owner": "alpha-aml",
    "retries": 1,
    "depends_on_past": False,
    "on_failure_callback": log_dag_failure,
}

with DAG(
    dag_id="aml_pipeline_audit",
    default_args=default_args,
    description="Hourly AML pipeline metrics snapshot",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aml", "audit"],
) as dag:
    PythonOperator(task_id="pipeline_audit", python_callable=pipeline_audit)
