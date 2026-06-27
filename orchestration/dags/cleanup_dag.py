"""Data lifecycle cleanup driven by configs/retention.json (single source of truth).

Deletes aged rows per policy: transactions, raw_transactions, flagged_transactions
(alerts), sar_reports, and account_window_metrics (age + row cap). Edit the values
in configs/retention.json — no code change needed.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Make `src` importable from the Airflow image (src is mounted at /opt/airflow/src).
if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")
from src.common.retention import load_retention, policy_max_rows, policy_value

from _event_log import log_dag_failure

POSTGRES_CONN_ID = "postgres_aml"


def _cutoff(value: int, unit: str) -> datetime:
    delta = timedelta(days=value) if unit == "days" else timedelta(hours=value)
    return datetime.utcnow() - delta


def cleanup_lifecycle(**context) -> dict:
    cfg = load_retention()
    policies = cfg.get("policies", {})
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    result: dict = {}
    # Age-based deletes for every configured policy table.
    for key, pol in policies.items():
        table = pol["table"]
        time_col = pol["time_column"]
        unit = pol.get("unit", "days")
        value = policy_value(key)
        cutoff = _cutoff(value, unit)
        cur.execute(f"DELETE FROM {table} WHERE {time_col} < %s", (cutoff,))
        result[f"{key}_deleted_by_age"] = cur.rowcount
        result[f"{key}_retention"] = f"{value} {unit}"

    # Row-cap enforcement for window metrics (high-churn table).
    metrics_cap = policy_max_rows("account_window_metrics", 0)
    overflow_deleted = 0
    if metrics_cap > 0:
        cur.execute("SELECT COUNT(*) FROM aml.account_window_metrics")
        remaining = cur.fetchone()[0]
        if remaining > metrics_cap:
            overflow = remaining - metrics_cap
            cur.execute(
                """
                DELETE FROM aml.account_window_metrics
                WHERE id IN (
                    SELECT id FROM aml.account_window_metrics
                    ORDER BY computed_at ASC
                    LIMIT %s
                )
                """,
                (overflow,),
            )
            overflow_deleted = cur.rowcount
    result["account_window_metrics_deleted_by_cap"] = overflow_deleted
    result["account_window_metrics_max_rows"] = metrics_cap

    conn.commit()
    cur.close()
    conn.close()
    print(result)
    return result


default_args = {
    "owner": "alpha-aml",
    "retries": 1,
    "depends_on_past": False,
    "on_failure_callback": log_dag_failure,
}

with DAG(
    dag_id="cleanup_dag",
    default_args=default_args,
    description="Retention cleanup driven by configs/retention.json (txn/alerts/SAR 90d, raw 24h, metrics 4h+cap)",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aml", "lifecycle"],
) as dag:
    PythonOperator(task_id="lifecycle_cleanup", python_callable=cleanup_lifecycle)
