"""Shared retention-policy loader.

Single source of truth lives in ``configs/retention.json``. The cleanup DAG and
the Streamlit dashboard both import this module so a value only ever changes in
one place. Per-policy environment variables still override the file when set
(handy for one-off ops without editing the file).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# Map policy key -> (env var that overrides the file value)
ENV_OVERRIDES = {
    "transactions": "TXN_RETENTION_DAYS",
    "raw_transactions": "RAW_RETENTION_HOURS",
    "flagged_transactions": "FLAGGED_RETENTION_DAYS",
    "sar_reports": "SAR_RETENTION_DAYS",
    "account_window_metrics": "METRICS_RETENTION_HOURS",
}

_FALLBACK: dict = {
    "policies": {
        "transactions": {"label": "Transactions", "table": "aml.transactions", "time_column": "ingested_at", "value": 90, "unit": "days"},
        "raw_transactions": {"label": "Raw transactions", "table": "aml.raw_transactions", "time_column": "ingested_at", "value": 24, "unit": "hours"},
        "flagged_transactions": {"label": "Alerts (flagged)", "table": "aml.flagged_transactions", "time_column": "flagged_at", "value": 180, "unit": "days"},
        "sar_reports": {"label": "SAR reports", "table": "aml.sar_reports", "time_column": "created_at", "value": 180, "unit": "days"},
        "alert_transaction_archive": {"label": "Alert transaction archive", "table": "aml.alert_transaction_archive", "time_column": "archived_at", "value": 180, "unit": "days"},
        "account_window_metrics": {"label": "Account window metrics", "table": "aml.account_window_metrics", "time_column": "computed_at", "value": 4, "unit": "hours", "max_rows": 50000},
    },
    "guards": {"disk_warn_pct": 80, "disk_critical_pct": 90, "pg_db_warn_gb": 30, "metrics_emergency_max_rows": 200000},
}


def _candidate_paths():
    env_path = os.getenv("RETENTION_CONFIG_PATH")
    if env_path:
        yield Path(env_path)
    yield Path("/app/configs/retention.json")
    yield Path("/opt/airflow/configs/retention.json")
    yield Path(__file__).resolve().parents[2] / "configs" / "retention.json"


def load_retention() -> dict:
    """Return the retention config dict, falling back to built-in defaults."""
    for path in _candidate_paths():
        try:
            if path and path.is_file():
                with open(path) as fh:
                    return json.load(fh)
        except (OSError, ValueError):
            continue
    return _FALLBACK


def policy_value(key: str) -> int:
    """Resolve a policy's numeric value: env override wins, then file, then fallback."""
    cfg = load_retention()
    file_val = cfg.get("policies", {}).get(key, {}).get(
        "value", _FALLBACK["policies"].get(key, {}).get("value", 0)
    )
    env_name = ENV_OVERRIDES.get(key)
    if env_name and os.getenv(env_name):
        try:
            return int(os.getenv(env_name))
        except ValueError:
            pass
    return int(file_val)


def policy_max_rows(key: str, default: int = 0) -> int:
    cfg = load_retention()
    return int(cfg.get("policies", {}).get(key, {}).get("max_rows", default))


def guard(key: str, default: int = 0) -> int:
    cfg = load_retention()
    return int(cfg.get("guards", {}).get(key, default))
