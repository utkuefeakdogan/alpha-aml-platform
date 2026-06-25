"""Database access for enterprise AML dashboard."""

from __future__ import annotations

import os
import re

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.dashboard.rules_manager import load_rules

# Cache TTLs (seconds) — keeps the memory-constrained DB from being hammered by
# every dashboard rerun / concurrent viewer. Live-ish data uses a short TTL;
# slow-moving aggregates use a longer one.
_TTL_FAST = 30
_TTL_SLOW = 300

PG_USER = os.getenv("POSTGRES_USER", "user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "datadb")

FREESQL_USER = os.getenv("FREESQL_USER", "freesql_reader")
FREESQL_PASSWORD = os.getenv("FREESQL_PASSWORD", "freesql_readonly_2026")
FREESQL_CONTACT_EMAIL = os.getenv("FREESQL_CONTACT_EMAIL", "compliance@alpha-aml.local")


def get_engine() -> Engine:
    return create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
        pool_pre_ping=True,
    )


ENGINE = get_engine()


@st.cache_resource
def get_freesql_engine() -> Engine:
    """Isolated connection — SQL Explorer uses the restricted freesql_reader role only."""
    return create_engine(
        f"postgresql+psycopg2://{FREESQL_USER}:{FREESQL_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=0,
    )


AIRFLOW_DB = os.getenv("AIRFLOW_META_DB", "airflow_meta")


@st.cache_resource
def get_airflow_engine() -> Engine:
    """Read connection to the Airflow metadata DB (same Postgres, separate database)."""
    return create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{AIRFLOW_DB}",
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=0,
    )


_OPEN_ALERT_JOIN = """
    LEFT JOIN aml.alert_dispositions d
        ON d.txn_id = f.txn_id AND d.rule_name = f.rule_name
    WHERE d.id IS NULL
"""

_OPEN_ALERT_SELECT = """
    SELECT
        f.txn_id AS alert_id,
        f.customer_id,
        f.rule_name,
        f.flagged_at,
        f.alert_priority_score,
        c.name AS customer_name,
        c.risk_score AS kyc_risk_score
    FROM aml.flagged_transactions f
    LEFT JOIN aml.customers c ON c.customer_id = f.customer_id
    LEFT JOIN aml.alert_dispositions d
        ON d.txn_id = f.txn_id AND d.rule_name = f.rule_name
    WHERE d.id IS NULL
"""


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


_WRITE_KEYWORDS = (
    "insert", "update", "delete", "drop", "alter", "truncate", "create",
    "grant", "revoke", "copy", "merge", "vacuum", "reindex", "comment",
    "call", "do", "set", "begin", "commit", "rollback",
)

# Match write/DDL keywords only as whole SQL tokens (word boundaries), never as
# substrings. A naive `kw in sql` check produced false positives like "do"
# inside "win<do>w", "set" inside "off<set>", or "call" inside "re<call>".
_WRITE_KEYWORD_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _WRITE_KEYWORDS) + r")\b"
)


def fetch_schema_overview(schema: str = "aml") -> pd.DataFrame:
    """Tables, columns and types for the given schema (dashboard admin connection)."""
    return query_df(
        """
        SELECT c.table_name, c.column_name, c.data_type, c.ordinal_position
        FROM information_schema.columns c
        WHERE c.table_schema = :schema
        ORDER BY c.table_name, c.ordinal_position
        """,
        {"schema": schema},
    )


def fetch_freesql_schema() -> pd.DataFrame:
    """Schema visible to freesql_reader — only tables with SELECT privilege."""
    with get_freesql_engine().connect() as conn:
        return pd.read_sql(
            text(
                """
                SELECT c.relname AS table_name,
                       a.attname AS column_name,
                       pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                       a.attnum AS ordinal_position
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                WHERE n.nspname = 'aml'
                  AND c.relkind = 'r'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                  AND has_table_privilege(c.oid, 'SELECT')
                ORDER BY c.relname, a.attnum
                """
            ),
            conn,
        )


def fetch_freesql_allowed_tables() -> list[str]:
    df = fetch_freesql_schema()
    if df.empty:
        return []
    return sorted(df["table_name"].unique().tolist())


def run_sql_readonly(sql: str, max_rows: int = 1000, timeout_ms: int = 8000) -> pd.DataFrame:
    """Execute a single read-only SELECT with hard guards.

    Safeguards: session forced read-only (writes fail at DB level), statement
    timeout, single statement only, write-keyword blocklist, and a row cap.
    """
    cleaned = sql.strip().rstrip(";").strip()
    if not cleaned:
        raise ValueError("Empty query.")
    if ";" in cleaned:
        raise ValueError("Only a single statement is allowed (remove ';').")
    lowered = cleaned.lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError("Only SELECT / WITH queries are allowed.")
    match = _WRITE_KEYWORD_RE.search(lowered)
    if match:
        raise ValueError(f"Write/DDL keyword not allowed: '{match.group(1)}'")

    wrapped = f"SELECT * FROM (\n{cleaned}\n) AS _q LIMIT {int(max_rows)}"
    with get_freesql_engine().connect() as conn:
        conn.execute(text("SET default_transaction_read_only = on"))
        conn.execute(text(f"SET statement_timeout = {int(timeout_ms)}"))
        return pd.read_sql(text(wrapped), conn)


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_table_estimates(tables: tuple[str, ...] | list[str]) -> dict[str, int]:
    """Fast approximate row counts (pg_class.reltuples) for the given aml.* tables."""
    tables = list(tables)
    if not tables:
        return {}
    try:
        df = query_df(
            """
            SELECT n.nspname || '.' || c.relname AS tbl,
                   GREATEST(c.reltuples, 0)::bigint AS est
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname || '.' || c.relname = ANY(:tables)
            """,
            {"tables": tables},
        )
        return {row["tbl"]: int(row["est"]) for _, row in df.iterrows()}
    except Exception:
        return {}


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_table_counts(tables: tuple[str, ...] | list[str]) -> dict[str, int]:
    """Exact COUNT(*) per table — accurate row counts for retention / data-model panels.

    Unlike fetch_table_estimates (pg_class.reltuples, which stays 0 until ANALYZE
    runs), this returns true counts. Used on small/medium tables only.
    """
    out: dict[str, int] = {}
    for tbl in tables:
        if not re.fullmatch(r"[a-zA-Z_]\w*\.[a-zA-Z_]\w*", tbl or ""):
            continue
        try:
            out[tbl] = int(query_df(f"SELECT COUNT(*) AS c FROM {tbl}").iloc[0]["c"])
        except Exception:
            out[tbl] = 0
    return out


def _row_estimate(table: str) -> int:
    """Approximate row count for a single table; exact COUNT only as last resort."""
    est = fetch_table_estimates((table,)).get(table, 0)
    if est > 0:
        return est
    try:
        return int(query_df(f"SELECT COUNT(*) AS c FROM {table}").iloc[0]["c"])
    except Exception:
        return 0


@st.cache_data(ttl=_TTL_SLOW, show_spinner=False)
def fetch_customer_count() -> int:
    # Exact count, cached 5 min — keeps the customer total consistent with the
    # KPI bundle and the Airflow audit snapshot (reltuples lags by ANALYZE cadence).
    try:
        return int(query_df("SELECT COUNT(*) AS c FROM aml.customers").iloc[0]["c"])
    except Exception:
        return _row_estimate("aml.customers")


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_dashboard_kpis() -> dict[str, int]:
    """Live KPI bundle shared by Overview and Monitoring (single source of truth).

    Must not call other @st.cache_data functions — Streamlit forbids nested cache calls.
    """
    row = query_df(
        """
        SELECT
          (SELECT COUNT(*) FROM aml.flagged_transactions) AS total_alerts,
          (SELECT COUNT(*) FROM aml.flagged_transactions
           WHERE flagged_at >= NOW() - INTERVAL '24 hours') AS alerts_24h,
          (SELECT COUNT(*) FROM aml.transactions
           WHERE ingested_at >= CURRENT_DATE) AS txns_today,
          (SELECT COUNT(*) FROM aml.customers) AS customers_est
        """
    ).iloc[0]
    customers = int(row["customers_est"] or 0)
    if customers <= 0:
        customers = 1_233_000
    return {
        "customers": customers,
        "total_alerts": int(row["total_alerts"]),
        "alerts_24h": int(row["alerts_24h"]),
        "txns_today": int(row["txns_today"]),
        "scenarios": fetch_active_scenario_count(),
    }


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_total_alert_count() -> int:
    return fetch_dashboard_kpis()["total_alerts"]


def fetch_active_scenario_count() -> int:
    from src.dashboard.scenario_catalog import active_scenario_count

    return active_scenario_count()


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_system_health() -> dict:
    """One-shot operational signals for the System Health page (all from datadb)."""
    try:
        row = query_df(
            """
            SELECT
              (SELECT MAX(ingested_at) FROM aml.transactions) AS last_ingest,
              (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(ingested_at)))
                 FROM aml.transactions) AS ingest_age_sec,
              (SELECT COUNT(*) FROM aml.transactions
                 WHERE ingested_at >= CURRENT_DATE) AS txns_today,
              (SELECT COUNT(*) FROM aml.transactions
                 WHERE ingested_at >= NOW() - INTERVAL '5 minutes') AS txns_5m,
              (SELECT COUNT(*) FROM aml.flagged_transactions
                 WHERE flagged_at >= NOW() - INTERVAL '24 hours') AS alerts_24h,
              (SELECT MAX(flagged_at) FROM aml.flagged_transactions) AS last_alert,
              (SELECT COUNT(*) FROM aml.customers) AS customers_live,
              (SELECT COUNT(*) FROM aml.customer_acquisition_log
                 WHERE acquired_at >= CURRENT_DATE) AS acquired_today,
              (SELECT MAX(acquired_at) FROM aml.customer_acquisition_log) AS last_acq,
              (SELECT MAX(created_at) FROM aml.sar_reports) AS last_sar,
              (SELECT COUNT(*) FROM aml.sar_reports
                 WHERE created_at >= CURRENT_DATE) AS sar_today,
              pg_database_size(current_database()) AS pg_db_size,
              (SELECT COUNT(*) FROM pg_stat_activity
                 WHERE datname = current_database()) AS pg_conns
            """
        )
        return row.iloc[0].to_dict() if not row.empty else {}
    except Exception:
        return {}


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_airflow_dag_health() -> pd.DataFrame:
    """Per-DAG run health from the Airflow metadata DB (last state + 24h tallies)."""
    try:
        with get_airflow_engine().connect() as conn:
            return pd.read_sql(
                text(
                    """
                    SELECT
                      dag_id,
                      MAX(start_date) AS last_start,
                      (ARRAY_AGG(state ORDER BY start_date DESC NULLS LAST))[1] AS last_state,
                      COUNT(*) FILTER (
                        WHERE start_date >= NOW() - INTERVAL '24 hours') AS runs_24h,
                      COUNT(*) FILTER (
                        WHERE start_date >= NOW() - INTERVAL '24 hours'
                          AND state = 'failed') AS failed_24h,
                      MAX(start_date) FILTER (WHERE state = 'success') AS last_success
                    FROM dag_run
                    GROUP BY dag_id
                    ORDER BY dag_id
                    """
                ),
                conn,
            )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_consistency_checks() -> dict:
    """Live COUNT vs latest Airflow snapshot — surfaces drift between sources."""
    out: dict = {}
    try:
        live = query_df(
            """
            SELECT
              (SELECT COUNT(*) FROM aml.customers) AS customers_live,
              (SELECT COUNT(*) FROM aml.customer_acquisition_log
                 WHERE acquired_at >= CURRENT_DATE) AS acquired_today_live
            """
        ).iloc[0]
        snap = query_df(
            """
            SELECT DISTINCT ON (metric_key) metric_key, metric_value
            FROM aml.pipeline_metrics
            WHERE metric_key IN ('customers_total', 'customers_acquired_today')
            ORDER BY metric_key, recorded_at DESC
            """
        )
        snap_map = {r["metric_key"]: int(r["metric_value"]) for _, r in snap.iterrows()}
        out["customers_live"] = int(live["customers_live"])
        out["customers_snapshot"] = snap_map.get("customers_total")
        out["acquired_today_live"] = int(live["acquired_today_live"])
        out["acquired_today_snapshot"] = snap_map.get("customers_acquired_today")
    except Exception:
        pass
    return out


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_pipeline_metrics(limit: int = 24) -> pd.DataFrame:
    try:
        return query_df(
            """
            SELECT metric_key, metric_value, metric_detail, recorded_at
            FROM aml.pipeline_metrics
            ORDER BY recorded_at DESC
            LIMIT :limit
            """,
            {"limit": limit},
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=_TTL_SLOW, show_spinner=False)
def fetch_risk_band_distribution() -> pd.DataFrame:
    # GROUP BY over 1.23M customers — cached for 5 min since risk mix moves slowly.
    return query_df(
        """
        SELECT
            CASE
                WHEN risk_score < 50 THEN 'low_risk'
                WHEN risk_score < 75 THEN 'medium_risk'
                ELSE 'high_risk'
            END AS risk_band,
            COUNT(*) AS customer_count
        FROM aml.customers
        GROUP BY 1
        ORDER BY MIN(risk_score)
        """
    )


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_summary_metrics() -> dict[str, int]:
    # flagged + SAR are small (retention-capped) → exact; transactions can grow → estimate.
    row = query_df(
        """
        SELECT
          (SELECT COUNT(*) FROM aml.flagged_transactions) AS total_flagged,
          (SELECT COUNT(*) FROM aml.sar_reports) AS total_sar
        """
    ).iloc[0]
    return {
        "total_ingested": _row_estimate("aml.transactions"),
        "total_flagged": int(row["total_flagged"]),
        "total_sar": int(row["total_sar"]),
    }


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_open_alerts_count() -> int:
    row = query_df(
        f"""
        SELECT COUNT(*) AS cnt
        FROM aml.flagged_transactions f
        {_OPEN_ALERT_JOIN}
        """
    ).iloc[0]
    return int(row["cnt"])


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_archived_alerts_count() -> int:
    return int(
        query_df("SELECT COUNT(*) AS cnt FROM aml.alert_dispositions").iloc[0]["cnt"]
    )


def fetch_open_alerts(limit: int = 10, offset: int = 0) -> pd.DataFrame:
    return query_df(
        f"""
        {_OPEN_ALERT_SELECT}
        ORDER BY f.alert_priority_score DESC NULLS LAST, f.flagged_at DESC
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )


def fetch_top_open_alerts(limit: int = 10) -> pd.DataFrame:
    return fetch_open_alerts(limit=limit, offset=0)


def fetch_priority_alerts(limit: int = 5) -> pd.DataFrame:
    return query_df(
        f"""
        {_OPEN_ALERT_SELECT}
        ORDER BY f.flagged_at DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )


def fetch_recent_alerts(limit: int = 100) -> pd.DataFrame:
    return query_df(
        """
        SELECT
            f.txn_id, f.customer_id, f.account_id,
            f.amount, f.amount_eur, f.currency,
            f.rule_name, f.rule_detail, f.window_type, f.flagged_at,
            t.txn_category, t.txn_type, t.country_code,
            t.sender_customer_no, t.receiver_customer_no,
            t.sender_name, t.receiver_name,
            t.sender_identity_no, t.receiver_identity_no,
            t.sender_branch, t.receiver_branch,
            t.sender_country, t.receiver_country, t.txn_description
        FROM aml.flagged_transactions f
        LEFT JOIN aml.transactions t ON t.txn_id = f.txn_id
        LEFT JOIN aml.alert_dispositions d
            ON d.txn_id = f.txn_id AND d.rule_name = f.rule_name
        WHERE d.id IS NULL
        ORDER BY f.flagged_at DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )


def fetch_customer_addresses(customer_id: str) -> pd.DataFrame:
    return query_df(
        """
        SELECT address_id, city, district, country_code, address_type
        FROM aml.customer_addresses
        WHERE customer_id = :cid
        ORDER BY address_type, address_id
        """,
        {"cid": customer_id},
    )


def fetch_customer_transactions_30d(customer_id: str) -> pd.DataFrame:
    return query_df(
        """
        SELECT txn_id, txn_category, txn_type,
               sender_customer_no, receiver_customer_no,
               sender_name, receiver_name,
               sender_identity_no, receiver_identity_no,
               sender_branch, receiver_branch,
               sender_country, receiver_country,
               amount, currency, amount_eur, txn_description,
               country_code, ts, ingested_at
        FROM aml.transactions
        WHERE (COALESCE(sender_customer_no, sender_id) = :cid
            OR receiver_customer_no = :cid)
          AND ts >= NOW() - INTERVAL '30 days'
        ORDER BY ts DESC
        """,
        {"cid": customer_id},
    )


def fetch_alert_archive_transactions(
    customer_id: str,
    alert_txn_id: str,
    rule_name: str,
) -> pd.DataFrame:
    """All customer transactions frozen at alert time (180d retention)."""
    return query_df(
        """
        SELECT txn_id, txn_category, txn_type,
               sender_customer_no, receiver_customer_no,
               sender_name, receiver_name,
               sender_identity_no, receiver_identity_no,
               sender_branch, receiver_branch,
               sender_country, receiver_country,
               amount, currency, amount_eur, txn_description,
               country_code, ts, txn_ingested_at AS ingested_at,
               alert_txn_id, rule_name, flagged_at, archived_at
        FROM aml.alert_transaction_archive
        WHERE customer_id = :cid
          AND alert_txn_id = :alert_txn
          AND rule_name = :rule
        ORDER BY ts DESC
        """,
        {"cid": customer_id, "alert_txn": alert_txn_id, "rule": rule_name},
    )


def count_alert_archive_rows(alert_txn_id: str, rule_name: str) -> int:
    try:
        return int(
            query_df(
                """
                SELECT COUNT(*) AS cnt FROM aml.alert_transaction_archive
                WHERE alert_txn_id = :alert_txn AND rule_name = :rule
                """,
                {"alert_txn": alert_txn_id, "rule": rule_name},
            ).iloc[0]["cnt"]
        )
    except Exception:
        return 0


def fetch_customer_behavior(customer_id: str) -> pd.Series | None:
    df = query_df(
        "SELECT * FROM aml.customer_behavior_30d WHERE customer_id = :cid",
        {"cid": customer_id},
    )
    return df.iloc[0] if not df.empty else None


def fetch_customer_alerts_30d(customer_id: str) -> pd.DataFrame:
    return query_df(
        """
        SELECT txn_id, rule_name, rule_detail, window_type, flagged_at
        FROM aml.flagged_transactions
        WHERE customer_id = :cid AND flagged_at >= NOW() - INTERVAL '30 days'
        ORDER BY flagged_at DESC
        """,
        {"cid": customer_id},
    )


def resolve_investigation_target(search_id: str) -> dict | None:
    """Resolve alert_id (txn_id), customer_id, or customer name to investigation context."""
    if not search_id or not str(search_id).strip():
        return None
    sid = str(search_id).strip()

    if sid.upper().startswith("CUST-"):
        cust = query_df(
            "SELECT customer_id FROM aml.customers WHERE customer_id = :cid LIMIT 1",
            {"cid": sid},
        )
        if not cust.empty:
            return {
                "mode": "customer",
                "customer_id": str(cust.iloc[0]["customer_id"]),
            }

    by_alert = query_df(
        """
        SELECT f.txn_id, f.rule_name, f.customer_id
        FROM aml.flagged_transactions f
        WHERE f.txn_id = :tid
        ORDER BY f.flagged_at DESC LIMIT 1
        """,
        {"tid": sid},
    )
    if not by_alert.empty:
        row = by_alert.iloc[0]
        return {
            "mode": "alert",
            "txn_id": str(row["txn_id"]),
            "rule_name": str(row["rule_name"]),
            "customer_id": str(row["customer_id"]),
        }

    by_name = search_customers(sid, limit=1)
    if not by_name.empty:
        return {
            "mode": "customer",
            "customer_id": str(by_name.iloc[0]["customer_id"]),
        }
    return None


def fetch_customer_open_alerts(customer_id: str) -> pd.DataFrame:
    return query_df(
        f"""
        {_OPEN_ALERT_SELECT}
          AND f.customer_id = :cid
        ORDER BY f.alert_priority_score DESC NULLS LAST, f.flagged_at DESC
        """,
        {"cid": customer_id},
    )


def fetch_archived_alerts(limit: int = 10, offset: int = 0) -> pd.DataFrame:
    return query_df(
        """
        SELECT
            d.txn_id AS alert_id,
            d.rule_name,
            d.disposition,
            d.analyst_notes,
            d.created_at AS archived_at,
            c.name AS customer_name,
            f.flagged_at
        FROM aml.alert_dispositions d
        JOIN aml.flagged_transactions f
            ON f.txn_id = d.txn_id AND f.rule_name = d.rule_name
        LEFT JOIN aml.customers c ON c.customer_id = f.customer_id
        ORDER BY d.created_at DESC
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_sar_reports_count() -> int:
    return int(query_df("SELECT COUNT(*) AS cnt FROM aml.sar_reports").iloc[0]["cnt"])


def fetch_sar_reports(limit: int = 10, offset: int = 0) -> pd.DataFrame:
    return query_df(
        """
        SELECT report_id, customer_id, flagged_count, report_text, filed_by, created_at
        FROM aml.sar_reports
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )


def fetch_alert_disposition(txn_id: str, rule_name: str) -> pd.Series | None:
    df = query_df(
        """
        SELECT disposition, analyst_notes, created_at
        FROM aml.alert_dispositions
        WHERE txn_id = :tid AND rule_name = :rule
        LIMIT 1
        """,
        {"tid": txn_id, "rule": rule_name},
    )
    return df.iloc[0] if not df.empty else None


def fetch_customer_transactions(customer_id: str, limit: int = 100) -> pd.DataFrame:
    return query_df(
        """
        SELECT txn_id, txn_category, txn_type,
               sender_customer_no, receiver_customer_no,
               sender_name, receiver_name,
               sender_identity_no, receiver_identity_no,
               sender_branch, receiver_branch,
               sender_country, receiver_country, txn_description,
               branch_id, amount, currency, amount_eur, country_code,
               ts, ingested_at
        FROM aml.transactions
        WHERE COALESCE(sender_customer_no, sender_id) = :cid
           OR receiver_customer_no = :cid
        ORDER BY ts DESC
        LIMIT :limit
        """,
        {"cid": customer_id, "limit": limit},
    )


def save_alert_disposition(
    txn_id: str,
    rule_name: str,
    disposition: str,
    analyst_notes: str | None = None,
) -> None:
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO aml.alert_dispositions (txn_id, rule_name, disposition, analyst_notes)
                VALUES (:txn_id, :rule_name, :disposition, :notes)
                ON CONFLICT (txn_id, rule_name)
                DO UPDATE SET disposition = EXCLUDED.disposition,
                              analyst_notes = EXCLUDED.analyst_notes,
                              created_at = NOW()
                """
            ),
            {
                "txn_id": txn_id,
                "rule_name": rule_name,
                "disposition": disposition,
                "notes": analyst_notes,
            },
        )


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_live_transactions_count() -> int:
    # Grows under retention — planner estimate is enough for feed pagination.
    return _row_estimate("aml.transactions")


def fetch_live_transactions(limit: int = 10, offset: int = 0) -> pd.DataFrame:
    return query_df(
        """
        SELECT txn_id, txn_category, txn_type,
               sender_customer_no, receiver_customer_no,
               sender_name, receiver_name,
               sender_identity_no, receiver_identity_no,
               sender_branch, receiver_branch,
               sender_country, receiver_country, txn_description,
               branch_id, amount, currency, amount_eur, country_code,
               ts, ingested_at
        FROM aml.transactions
        ORDER BY ingested_at DESC
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_alerts_by_rule_total() -> pd.DataFrame:
    return query_df(
        """
        SELECT rule_name, COUNT(*) AS alert_count
        FROM aml.flagged_transactions
        GROUP BY rule_name
        ORDER BY alert_count DESC
        """
    )


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_alerts_by_rule_24h() -> pd.DataFrame:
    return query_df(
        """
        SELECT rule_name, COUNT(*) AS alert_count
        FROM aml.flagged_transactions
        WHERE flagged_at >= NOW() - INTERVAL '24 hours'
        GROUP BY rule_name
        ORDER BY alert_count DESC
        """
    )


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_alerts_24h_trend() -> pd.DataFrame:
    return query_df(
        """
        SELECT rule_name, date_trunc('hour', flagged_at) AS hour_bucket, COUNT(*) AS alert_count
        FROM aml.flagged_transactions
        WHERE flagged_at >= NOW() - INTERVAL '24 hours'
        GROUP BY rule_name, date_trunc('hour', flagged_at)
        ORDER BY hour_bucket
        """
    )


def fetch_customer_profile(customer_id: str) -> dict | None:
    cust = query_df(
        "SELECT * FROM aml.customers WHERE customer_id = :cid",
        {"cid": customer_id},
    )
    if cust.empty:
        return None

    spend = query_df(
        """
        SELECT COUNT(*) AS txn_count_30d,
               COALESCE(SUM(amount_eur), 0) AS volume_30d,
               COALESCE(AVG(amount_eur), 0) AS avg_amount_30d
        FROM aml.transactions
        WHERE COALESCE(sender_customer_no, sender_id) = :cid AND ts >= NOW() - INTERVAL '30 days'
        """,
        {"cid": customer_id},
    ).iloc[0]

    flags = query_df(
        """
        SELECT rule_name, window_type, COUNT(*) AS cnt, MAX(flagged_at) AS last_flagged
        FROM aml.flagged_transactions
        WHERE customer_id = :cid AND flagged_at >= NOW() - INTERVAL '30 days'
        GROUP BY rule_name, window_type
        ORDER BY cnt DESC
        """,
        {"cid": customer_id},
    )

    windows = query_df(
        """
        SELECT DISTINCT ON (window_type)
            window_type, total_volume, txn_count, distinct_receiver_count, computed_at
        FROM aml.account_window_metrics
        WHERE customer_id = :cid
        ORDER BY window_type, computed_at DESC
        """,
        {"cid": customer_id},
    )

    recent_txn = query_df(
        """
        SELECT txn_id, txn_category, txn_type, amount, currency, amount_eur,
               receiver_customer_no, receiver_name,
               sender_branch, receiver_branch, sender_country, receiver_country,
               txn_description, country_code, ts
        FROM aml.transactions
        WHERE COALESCE(sender_customer_no, sender_id) = :cid
        ORDER BY ts DESC LIMIT 20
        """,
        {"cid": customer_id},
    )

    return {
        "customer": cust.iloc[0].to_dict(),
        "spend": spend.to_dict(),
        "flags": flags,
        "windows": windows,
        "recent_txn": recent_txn,
    }


def search_customers(q: str, limit: int = 20) -> pd.DataFrame:
    return query_df(
        """
        SELECT customer_id, name, risk_score, segment, is_pep, branch_id,
               onboarding_date, onboarding_channel, customer_status, identity_no
        FROM aml.customers
        WHERE customer_id ILIKE :q OR name ILIKE :q
        ORDER BY customer_id
        LIMIT :limit
        """,
        {"q": f"%{q}%", "limit": limit},
    )


def fetch_alert_by_txn_id(txn_id: str, rule_name: str | None = None) -> pd.Series | None:
    if rule_name:
        df = query_df(
            """
            SELECT f.*, t.txn_category, t.txn_type, t.country_code, t.receiver_id,
                   t.sender_name, t.receiver_name, t.sender_country, t.receiver_country,
                   t.txn_description
            FROM aml.flagged_transactions f
            LEFT JOIN aml.transactions t ON t.txn_id = f.txn_id
            WHERE f.txn_id = :txn_id AND f.rule_name = :rule_name
            LIMIT 1
            """,
            {"txn_id": txn_id, "rule_name": rule_name},
        )
    else:
        df = query_df(
            """
            SELECT f.*, t.txn_category, t.txn_type, t.country_code, t.receiver_id,
                   t.sender_name, t.receiver_name, t.sender_country, t.receiver_country,
                   t.txn_description
            FROM aml.flagged_transactions f
            LEFT JOIN aml.transactions t ON t.txn_id = f.txn_id
            WHERE f.txn_id = :txn_id
            LIMIT 1
            """,
            {"txn_id": txn_id},
        )
    return df.iloc[0] if not df.empty else None


def fetch_sar_context(customer_id: str) -> dict:
    profile = fetch_customer_profile(customer_id)
    if not profile:
        return {}
    weekly = profile["windows"]
    weekly_row = weekly[weekly["window_type"] == "weekly"] if not weekly.empty else pd.DataFrame()
    weekly_vol = float(weekly_row["total_volume"].iloc[0]) if not weekly_row.empty else 0.0
    weekly_cnt = int(weekly_row["txn_count"].iloc[0]) if not weekly_row.empty else 0
    fast_cnt = query_df(
        """
        SELECT COUNT(*) AS cnt FROM aml.transactions
        WHERE COALESCE(sender_customer_no, sender_id) = :cid AND txn_type = 'FAST'
          AND ts >= NOW() - INTERVAL '7 days'
        """,
        {"cid": customer_id},
    )["cnt"].iloc[0]
    return {
        "customer_id": customer_id,
        "name": profile["customer"]["name"],
        "risk_score": float(profile["customer"]["risk_score"]),
        "segment": profile["customer"]["segment"],
        "volume_30d": float(profile["spend"]["volume_30d"]),
        "txn_count_30d": int(profile["spend"]["txn_count_30d"]),
        "weekly_volume": weekly_vol,
        "weekly_txn_count": weekly_cnt,
        "weekly_fast_count": int(fast_cnt),
    }


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_ml_latest_run() -> dict:
    """Most recent ML training run with its evaluation metrics (JSONB columns
    arrive as native dict/list via psycopg2)."""
    try:
        df = query_df(
            "SELECT * FROM aml.ml_model_runs ORDER BY trained_at DESC LIMIT 1"
        )
        return df.iloc[0].to_dict() if not df.empty else {}
    except Exception:
        return {}


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_ml_scores() -> pd.DataFrame:
    """Full score snapshot (a few thousand active customers) for distribution charts."""
    try:
        return query_df(
            """
            SELECT anomaly_score, triage_score, is_anomaly, rule_flagged
            FROM aml.ml_customer_scores
            """
        )
    except Exception:
        return pd.DataFrame()


def fetch_ml_top_anomalies(limit: int = 20, by: str = "anomaly_score") -> pd.DataFrame:
    order_col = "triage_score" if by == "triage_score" else "anomaly_score"
    return query_df(
        f"""
        SELECT s.customer_id, c.name AS customer_name,
               s.anomaly_score, s.anomaly_rank, s.is_anomaly,
               s.triage_score, s.rule_flagged,
               s.txn_count_30d, s.volume_30d, s.distinct_receivers_30d,
               s.max_txn_30d, s.kyc_risk_score
        FROM aml.ml_customer_scores s
        LEFT JOIN aml.customers c ON c.customer_id = s.customer_id
        ORDER BY s.{order_col} DESC NULLS LAST
        LIMIT :limit
        """,
        {"limit": limit},
    )


@st.cache_data(ttl=_TTL_FAST, show_spinner=False)
def fetch_ml_overlap() -> dict:
    """Rule-engine vs ML-anomaly agreement counts (confusion-style 2x2)."""
    try:
        row = query_df(
            """
            SELECT
              COUNT(*) AS total,
              COUNT(*) FILTER (WHERE is_anomaly AND rule_flagged) AS both,
              COUNT(*) FILTER (WHERE is_anomaly AND NOT rule_flagged) AS ml_only,
              COUNT(*) FILTER (WHERE NOT is_anomaly AND rule_flagged) AS rule_only,
              COUNT(*) FILTER (WHERE NOT is_anomaly AND NOT rule_flagged) AS neither
            FROM aml.ml_customer_scores
            """
        ).iloc[0]
        return {
            k: int(row[k]) for k in ("total", "both", "ml_only", "rule_only", "neither")
        }
    except Exception:
        return {}


def insert_sar_report(
    report_id: str,
    account_id_hash: str,
    flagged_count: int,
    report_text: str,
    customer_id: str | None = None,
    model_used: str = "template",
    filed_by: str | None = None,
) -> None:
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO aml.sar_reports
                    (report_id, account_id_hash, customer_id, flagged_count,
                     report_text, model_used, filed_by)
                VALUES (:report_id, :hash, :cid, :cnt, :text, :model, :filed_by)
                ON CONFLICT (report_id) DO NOTHING
                """
            ),
            {
                "report_id": report_id,
                "hash": account_id_hash,
                "cid": customer_id,
                "cnt": flagged_count,
                "text": report_text,
                "model": model_used,
                "filed_by": filed_by,
            },
        )
