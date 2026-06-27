"""Shared Airflow failure callback that records DAG/task failures.

Importable from sibling DAG files (`from _event_log import log_dag_failure`)
because Airflow puts the DAGs folder on sys.path. The leading underscore keeps
the DAG parser from treating this helper as a DAG definition file.

Delegates to the single source of truth in src.common.event_log so the insert
schema stays in one place.
"""

from __future__ import annotations

import sys

if "/opt/airflow" not in sys.path:
    sys.path.insert(0, "/opt/airflow")

from src.common.event_log import log_dag_failure  # noqa: E402,F401
