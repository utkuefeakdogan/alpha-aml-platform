"""Intermittent customer acquisition — at least N new customers per UTC day."""

from __future__ import annotations

import logging
import os
import random
from datetime import date, datetime, timedelta, timezone

from psycopg2.extras import RealDictCursor

from src.generator.address_types import CITIES, random_address_types
from src.generator.customer_loader import (
    ALLOWED_COUNTRIES,
    ONBOARDING_CHANNELS,
    CustomerRecord,
    _connect,
)

logger = logging.getLogger(__name__)

CUSTOMERS_PER_DAY_MIN = int(os.getenv("CUSTOMERS_PER_DAY_MIN", "5"))
CUSTOMERS_PER_DAY_MAX = int(os.getenv("CUSTOMERS_PER_DAY_MAX", "8"))
HIGH_RISK_COUNTRIES = frozenset({"TR", "RU", "KP", "IR", "SY", "CU"})

_schedule_date: date | None = None
_scheduled_times: list[datetime] = []


def _kyc_risk_score(segment: str, is_pep: bool, channel: str, country: str) -> float:
    seg_pts = {"retail": 15, "premium": 30, "corporate": 45}.get(segment, 15)
    pep_pts = 25 if is_pep else 0
    ch_pts = {"Branch": 0, "Mobile": 5, "Web": 10, "ATM": 15}.get(channel, 0)
    country_pts = 10 if country in HIGH_RISK_COUNTRIES else 0
    return float(min(100, seg_pts + pep_pts + ch_pts + country_pts))


def _next_customer_id(cur) -> str:
    # Take the max of the NUMERIC part, not the raw string. A string MAX() on a
    # zero-padded VARCHAR sorts lexicographically, so a stray differently-padded
    # id (e.g. 8-digit "CUST-01233001") would make MAX() return the wrong row and
    # the generator would regenerate the same id forever (duplicate-key loop).
    # Padding is 7 digits to match the seeded 1.23M customers (CUST-0000001 …).
    cur.execute(
        "SELECT MAX(CAST(REGEXP_REPLACE(customer_id, '\\D', '', 'g') AS BIGINT)) AS max_num "
        "FROM aml.customers"
    )
    row = cur.fetchone()
    max_num = int(row["max_num"]) if row and row["max_num"] is not None else 0
    return f"CUST-{max_num + 1:07d}"


def _count_today(cur) -> int:
    cur.execute(
        """
        SELECT COUNT(*) AS cnt FROM aml.customer_acquisition_log
        WHERE acquired_at >= CURRENT_DATE
        """
    )
    return int(cur.fetchone()["cnt"])


def _ensure_schedule(now: datetime) -> None:
    global _schedule_date, _scheduled_times
    today = now.date()
    if _schedule_date == today and _scheduled_times:
        return
    _schedule_date = today
    _scheduled_times = []
    try:
        with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            already = _count_today(cur)
    except Exception as exc:
        logger.warning("Could not read acquisition count: %s", exc)
        already = 0
    remaining = max(0, CUSTOMERS_PER_DAY_MIN - already)
    if remaining == 0:
        return
    end_of_day = datetime.combine(today + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    start = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    span = max(60.0, (end_of_day - start).total_seconds())
    for _ in range(remaining):
        offset = random.uniform(0, span)
        _scheduled_times.append(start + timedelta(seconds=offset))
    _scheduled_times.sort()
    logger.info("Scheduled %d customer acquisitions for %s", remaining, today)


def _insert_customer(cur, cid: str) -> CustomerRecord:
    segment = random.choices(
        ["retail", "premium", "corporate"], weights=[70, 20, 10], k=1
    )[0]
    is_pep = random.random() < 0.04
    channel = random.choice(ONBOARDING_CHANNELS)
    country = random.choice(ALLOWED_COUNTRIES)
    risk = _kyc_risk_score(segment, is_pep, channel, country)
    identity = f"{random.randint(10000000000, 99999999999)}"
    name = f"Customer {cid.split('-')[-1]}"
    branch = f"BR-{random.randint(1, 12):03d}"
    onboard = date.today() - timedelta(days=random.randint(0, 365))

    cur.execute(
        """
        INSERT INTO aml.customers (
            customer_id, identity_no, name, onboarding_date, onboarding_channel,
            risk_score, segment, is_pep, branch_id, country, customer_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active')
        """,
        (cid, identity, name, onboard, channel, risk, segment, is_pep, branch, country),
    )
    for atype in random_address_types():
        cur.execute(
            """
            INSERT INTO aml.customer_addresses (customer_id, city, district, country_code, address_type)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                cid,
                random.choice(CITIES),
                f"District {random.randint(1, 20)}",
                country,
                atype,
            ),
        )
    cur.execute(
        "INSERT INTO aml.customer_acquisition_log (customer_id) VALUES (%s)",
        (cid,),
    )
    return CustomerRecord(
        customer_id=cid,
        name=name,
        identity_no=identity,
        branch_id=branch,
        country=country,
        onboarding_date=onboard,
        onboarding_channel=channel,
        customer_status="active",
        risk_score=risk,
        segment=segment,
        is_pep=is_pep,
    )


def maybe_acquire_customer(
    registry: dict[str, CustomerRecord],
    active_pool: dict[str, CustomerRecord],
) -> CustomerRecord | None:
    """Create a customer if a scheduled slot has passed. Returns new record or None."""
    now = datetime.now(timezone.utc)
    _ensure_schedule(now)
    while _scheduled_times and _scheduled_times[0] <= now:
        _scheduled_times.pop(0)
        try:
            with _connect() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cid = _next_customer_id(cur)
                    record = _insert_customer(cur, cid)
                conn.commit()
            registry[cid] = record
            active_pool[cid] = record
            logger.info("Acquired new customer %s (risk=%.1f)", cid, record.risk_score)
            return record
        except Exception as exc:
            logger.warning("Customer acquisition failed: %s", exc)
            return None
    return None
