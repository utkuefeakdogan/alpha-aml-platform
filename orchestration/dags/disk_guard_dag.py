"""Host disk and PostgreSQL size guard with emergency metrics cleanup."""

from __future__ import annotations

import logging
import os
import shutil
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Make `src` importable from the Airflow image (src is mounted at /opt/airflow/src).
if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")
from src.common.retention import guard

from _event_log import log_dag_failure

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_aml"
HOST_ROOT = os.getenv("DISK_GUARD_HOST_ROOT", "/host")
DISK_WARN_PCT = int(os.getenv("DISK_WARN_PCT", str(guard("disk_warn_pct", 80))))
DISK_CRITICAL_PCT = int(os.getenv("DISK_CRITICAL_PCT", str(guard("disk_critical_pct", 90))))
PG_DB_WARN_BYTES = int(os.getenv("PG_DB_WARN_BYTES", str(guard("pg_db_warn_gb", 30) * 1024**3)))
METRICS_EMERGENCY_MAX_ROWS = int(
    os.getenv("METRICS_EMERGENCY_MAX_ROWS", str(guard("metrics_emergency_max_rows", 200000)))
)


def _host_disk_usage() -> dict | None:
    path = HOST_ROOT if os.path.isdir(HOST_ROOT) else "/"
    try:
        total, used, free = shutil.disk_usage(path)
        pct = round(used / total * 100, 1) if total else 0.0
        return {
            "path": path,
            "total_gb": round(total / 1024**3, 2),
            "used_gb": round(used / 1024**3, 2),
            "free_gb": round(free / 1024**3, 2),
            "used_pct": pct,
        }
    except OSError as exc:
        logger.warning("Could not read disk usage from %s: %s", path, exc)
        return None


def _pg_sizes(hook: PostgresHook) -> dict:
    db_bytes = hook.get_first(
        "SELECT pg_database_size(current_database())"
    )[0]
    metrics_rows = hook.get_first(
        "SELECT COUNT(*) FROM aml.account_window_metrics"
    )[0]
    top_tables = hook.get_records(
        """
        SELECT relname, pg_total_relation_size(c.oid)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'aml' AND c.relkind = 'r'
        ORDER BY pg_total_relation_size(c.oid) DESC
        LIMIT 5
        """
    )
    return {
        "db_bytes": int(db_bytes),
        "db_gb": round(int(db_bytes) / 1024**3, 2),
        "metrics_rows": int(metrics_rows),
        "top_tables": [
            {"table": name, "size_gb": round(int(size) / 1024**3, 3)}
            for name, size in top_tables
        ],
    }


def disk_guard(**context) -> dict:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    disk = _host_disk_usage()
    pg = _pg_sizes(hook)

    actions: list[str] = []
    level = "ok"

    if disk:
        if disk["used_pct"] >= DISK_CRITICAL_PCT:
            level = "critical"
            actions.append(f"host_disk_critical_{disk['used_pct']}%")
        elif disk["used_pct"] >= DISK_WARN_PCT:
            level = "warn"
            actions.append(f"host_disk_warn_{disk['used_pct']}%")

    if pg["db_bytes"] >= PG_DB_WARN_BYTES:
        level = "critical" if level == "critical" else "warn"
        actions.append(f"pg_db_size_{pg['db_gb']}gb")

    emergency_truncated = 0
    if (
        (disk and disk["used_pct"] >= DISK_CRITICAL_PCT)
        or pg["metrics_rows"] > METRICS_EMERGENCY_MAX_ROWS
    ):
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("TRUNCATE aml.account_window_metrics")
        emergency_truncated = cur.rowcount if cur.rowcount >= 0 else pg["metrics_rows"]
        conn.commit()
        cur.close()
        conn.close()
        actions.append("emergency_truncate_account_window_metrics")
        level = "critical"

    result = {
        "level": level,
        "disk": disk,
        "postgres": pg,
        "actions": actions,
        "thresholds": {
            "disk_warn_pct": DISK_WARN_PCT,
            "disk_critical_pct": DISK_CRITICAL_PCT,
            "pg_db_warn_gb": round(PG_DB_WARN_BYTES / 1024**3, 1),
            "metrics_emergency_max_rows": METRICS_EMERGENCY_MAX_ROWS,
        },
    }
    if level == "critical":
        logger.error("DISK GUARD CRITICAL: %s", result)
    elif level == "warn":
        logger.warning("DISK GUARD WARN: %s", result)
    else:
        logger.info("DISK GUARD OK: %s", result)
    print(result)
    return result


default_args = {
    "owner": "alpha-aml",
    "retries": 0,
    "depends_on_past": False,
    "on_failure_callback": log_dag_failure,
}

with DAG(
    dag_id="disk_guard_dag",
    default_args=default_args,
    description="Monitor host disk / PG size; emergency-truncate metrics if critical",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aml", "ops", "disk"],
) as dag:
    PythonOperator(task_id="disk_guard_check", python_callable=disk_guard)
