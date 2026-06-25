"""Load customers from PostgreSQL via random sampling (no full in-memory universe)."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import date

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

PG_USER = os.getenv("POSTGRES_USER", "user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "datadb")

ONBOARDING_CHANNELS = ("Mobile", "Branch", "Web", "ATM")
ALLOWED_COUNTRIES = ["DE", "FR", "NL", "AT", "BE", "IT", "ES", "PL", "CH", "TR"]

_CUSTOMER_SELECT = """
    SELECT customer_id, name, identity_no, branch_id, country,
           onboarding_date, onboarding_channel, customer_status,
           risk_score, segment, is_pep
    FROM aml.customers
"""


@dataclass
class CustomerRecord:
    customer_id: str
    name: str
    identity_no: str
    branch_id: str
    country: str
    onboarding_date: date
    onboarding_channel: str
    customer_status: str = "active"
    risk_score: float = 0.0
    segment: str = "retail"
    is_pep: bool = False


def _connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def _row_to_record(row: dict) -> CustomerRecord:
    return CustomerRecord(
        customer_id=str(row["customer_id"]),
        name=str(row["name"]),
        identity_no=str(row["identity_no"]),
        branch_id=str(row["branch_id"]),
        country=str(row["country"] or "DE"),
        onboarding_date=row["onboarding_date"],
        onboarding_channel=str(row["onboarding_channel"] or "Branch"),
        customer_status=str(row["customer_status"] or "active"),
        risk_score=float(row["risk_score"] or 0),
        segment=str(row["segment"] or "retail"),
        is_pep=bool(row["is_pep"]),
    )


def fetch_random_active_customer(retries: int = 5, delay_sec: float = 2.0) -> CustomerRecord | None:
    """Single active customer via TABLESAMPLE-style random row."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    {_CUSTOMER_SELECT}
                    WHERE customer_status = 'active'
                    ORDER BY RANDOM()
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
            if row:
                return _row_to_record(row)
            return None
        except Exception as exc:
            last_err = exc
            logger.warning("fetch_random_active_customer %d/%d: %s", attempt, retries, exc)
            time.sleep(delay_sec)
    raise ConnectionError(f"Could not sample active customer: {last_err}")


def fetch_random_dormant_customer(retries: int = 5, delay_sec: float = 2.0) -> CustomerRecord | None:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    {_CUSTOMER_SELECT}
                    WHERE customer_status = 'dormant'
                    ORDER BY RANDOM()
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
            if row:
                return _row_to_record(row)
            return None
        except Exception as exc:
            last_err = exc
            logger.warning("fetch_random_dormant_customer %d/%d: %s", attempt, retries, exc)
            time.sleep(delay_sec)
    raise ConnectionError(f"Could not sample dormant customer: {last_err}")


def fetch_customer_by_id(customer_id: str) -> CustomerRecord | None:
    with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"{_CUSTOMER_SELECT} WHERE customer_id = %s", (customer_id,))
        row = cur.fetchone()
    return _row_to_record(row) if row else None


def count_flags_last_24h() -> int:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM aml.flagged_transactions
            WHERE flagged_at >= NOW() - INTERVAL '24 hours'
            """
        )
        return int(cur.fetchone()[0])


def flags_by_rule_last_7d() -> dict[str, int]:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT rule_name, COUNT(*)::int
            FROM aml.flagged_transactions
            WHERE flagged_at >= NOW() - INTERVAL '7 days'
            GROUP BY rule_name
            """
        )
        return {str(row[0]): int(row[1]) for row in cur.fetchall()}


def load_customers_from_db(limit: int = 500, retries: int = 3, delay_sec: float = 2.0) -> dict[str, CustomerRecord]:
    """Legacy small pool for offline fallback paths only."""
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    {_CUSTOMER_SELECT}
                    WHERE customer_status = 'active'
                    ORDER BY RANDOM()
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
            registry = {_row_to_record(r).customer_id: _row_to_record(r) for r in rows}
            logger.info("Loaded sample of %d customers from database", len(registry))
            return registry
        except Exception as exc:
            last_err = exc
            logger.warning("DB customer sample %d/%d: %s", attempt, retries, exc)
            time.sleep(delay_sec)
    raise ConnectionError(f"Could not load customers from Postgres: {last_err}")


def active_customers(registry: dict[str, CustomerRecord]) -> dict[str, CustomerRecord]:
    return {k: v for k, v in registry.items() if v.customer_status == "active"}


def build_fallback_registry(size: int = 200) -> dict[str, CustomerRecord]:
    """Offline fallback when Postgres is unavailable."""
    from faker import Faker

    fake = Faker(["de_DE", "tr_TR"])
    registry: dict[str, CustomerRecord] = {}
    for i in range(1, size + 1):
        cid = f"CUST-{i:05d}"
        registry[cid] = CustomerRecord(
            customer_id=cid,
            name=fake.name(),
            identity_no=f"{10000000000 + i}",
            branch_id=f"BR-{i % 12 + 1:03d}",
            country=ALLOWED_COUNTRIES[i % len(ALLOWED_COUNTRIES)],
            onboarding_date=date.today(),
            onboarding_channel=ONBOARDING_CHANNELS[i % 4],
            customer_status="active" if i <= 200 else "dormant",
        )
    return registry
