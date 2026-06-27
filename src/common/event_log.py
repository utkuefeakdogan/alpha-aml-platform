"""Central event/error logging into aml.event_log.

Two integration points:

  * ``install_pg_log_handler(source)`` attaches a logging handler that mirrors
    every WARNING+ record from a long-running service into the table. Existing
    ``logger.warning/error/exception`` call sites need no changes.
  * ``log_dag_failure(context)`` is used as an Airflow ``on_failure_callback`` so
    DAG/task failures land in the same place.

Everything here is strictly best-effort: a logging backend must never crash (or
slow down) the caller, so all DB errors are swallowed and reported to stderr
(never back through ``logging`` — that would recurse into this handler).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback

import psycopg2

_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
_PG_DB = os.getenv("POSTGRES_DB", "datadb")
_PG_USER = os.getenv("POSTGRES_USER", "user")
_PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
_CONNECT_TIMEOUT = int(os.getenv("EVENT_LOG_CONNECT_TIMEOUT", "3"))

_INSERT_SQL = (
    "INSERT INTO aml.event_log (source, level, logger, message, detail) "
    "VALUES (%s, %s, %s, %s, %s)"
)

# Re-entrancy guard: while we are writing an event we must not let any failure
# loop back through the logging machinery (which our own handler listens on).
_in_emit = False


def _stderr(msg: str) -> None:
    try:
        sys.stderr.write(f"[event_log] {msg}\n")
    except Exception:
        pass


def log_event(
    source: str,
    level: str,
    message: str,
    logger: str | None = None,
    detail: dict | None = None,
) -> None:
    """Best-effort insert of a single event row. Never raises."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=_PG_HOST,
            port=_PG_PORT,
            dbname=_PG_DB,
            user=_PG_USER,
            password=_PG_PASSWORD,
            connect_timeout=_CONNECT_TIMEOUT,
        )
        conn.autocommit = True
        detail_json = json.dumps(detail, default=str) if detail else None
        with conn.cursor() as cur:
            cur.execute(
                _INSERT_SQL,
                (source[:32], str(level)[:16], (logger or "")[:128], message, detail_json),
            )
    except Exception as exc:  # pragma: no cover - logging must never crash callers
        _stderr(f"insert failed: {exc}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


class PostgresLogHandler(logging.Handler):
    """Mirror WARNING+ log records into aml.event_log."""

    def __init__(self, source: str, level: int = logging.WARNING) -> None:
        super().__init__(level=level)
        self.source = source

    def emit(self, record: logging.LogRecord) -> None:
        global _in_emit
        if _in_emit or record.levelno < logging.WARNING:
            return
        _in_emit = True
        try:
            detail: dict = {}
            if record.exc_info:
                detail["traceback"] = "".join(traceback.format_exception(*record.exc_info))
            try:
                message = record.getMessage()
            except Exception:
                message = str(record.msg)
            log_event(
                self.source,
                record.levelname,
                message,
                logger=record.name,
                detail=detail or None,
            )
        except Exception as exc:  # pragma: no cover
            _stderr(f"emit failed: {exc}")
        finally:
            _in_emit = False


def install_pg_log_handler(source: str, level: int = logging.WARNING) -> None:
    """Attach a single PostgresLogHandler to the root logger (idempotent)."""
    root = logging.getLogger()
    for h in root.handlers:
        if isinstance(h, PostgresLogHandler):
            return
    root.addHandler(PostgresLogHandler(source, level=level))


def log_dag_failure(context: dict) -> None:
    """Airflow ``on_failure_callback``: record a failed task into aml.event_log."""
    try:
        ti = context.get("task_instance")
        dag_id = getattr(ti, "dag_id", None) or context.get("dag", None)
        task_id = getattr(ti, "task_id", None)
        exception = context.get("exception")
        detail = {
            "dag_id": str(dag_id),
            "task_id": str(task_id),
            "execution_date": str(context.get("execution_date") or context.get("logical_date")),
            "try_number": getattr(ti, "try_number", None),
            "exception": "".join(traceback.format_exception_only(type(exception), exception)).strip()
            if exception
            else None,
            "log_url": getattr(ti, "log_url", None),
        }
        message = f"{dag_id}.{task_id} failed"
        _log_dag_event(message, detail)
    except Exception as exc:  # pragma: no cover
        _stderr(f"dag failure callback error: {exc}")


def _log_dag_event(message: str, detail: dict) -> None:
    """Write a DAG failure via Airflow's PostgresHook when available.

    Falls back to a direct psycopg2 connection so the callback still works
    outside a fully configured Airflow runtime.
    """
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_aml")
        hook.run(
            _INSERT_SQL,
            parameters=("airflow", "ERROR", message[:128], message, json.dumps(detail, default=str)),
        )
    except Exception:
        log_event("airflow", "ERROR", message, logger=message[:128], detail=detail)
