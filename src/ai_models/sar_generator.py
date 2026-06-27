"""Generate Suspicious Activity Reports from flagged transactions."""

from __future__ import annotations

import hashlib
import logging
import os
import uuid
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

from src.common.event_log import install_pg_log_handler

logging.basicConfig(level=logging.INFO)
install_pg_log_handler("sar")
logger = logging.getLogger(__name__)

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "datadb")
PG_USER = os.getenv("POSTGRES_USER", "user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
MOCK_MODE = os.getenv("SAR_MOCK_MODE", "true").lower() in ("1", "true", "yes")
BATCH_LIMIT = int(os.getenv("SAR_BATCH_LIMIT", "20"))


def _connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def _hash_account(account_id: str) -> str:
    return hashlib.sha256(account_id.encode()).hexdigest()[:16]


def _existing_hashes(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT account_id_hash FROM aml.sar_reports")
        return {row[0] for row in cur.fetchall()}


def _fetch_flagged_groups(conn) -> list[dict]:
    existing = _existing_hashes(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT account_id, COUNT(*) AS cnt,
                   ARRAY_AGG(DISTINCT rule_name) AS rules,
                   MAX(amount) AS max_amount
            FROM aml.flagged_transactions
            GROUP BY account_id
            ORDER BY cnt DESC
            LIMIT %s
            """,
            (BATCH_LIMIT * 3,),
        )
        rows = cur.fetchall()
    return [r for r in rows if _hash_account(r["account_id"]) not in existing][:BATCH_LIMIT]


def _build_prompt(group: dict) -> str:
    account_hash = _hash_account(group["account_id"])
    rules = ", ".join(group["rules"] or [])
    return (
        f"Generate a concise AML Suspicious Activity Report (SAR) for compliance.\n"
        f"Account hash: {account_hash}\n"
        f"Flagged transactions: {group['cnt']}\n"
        f"Rules triggered: {rules}\n"
        f"Max amount (EUR): {group['max_amount']}\n"
        f"Do not include full account numbers or PII. Use professional regulatory tone."
    )


def _mock_sar(group: dict) -> str:
    account_hash = _hash_account(group["account_id"])
    rules = ", ".join(group["rules"] or [])
    return (
        f"SUSPICIOUS ACTIVITY REPORT (MOCK)\n"
        f"Report Date: {datetime.now(timezone.utc).isoformat()}\n"
        f"Subject Account (hashed): {account_hash}\n\n"
        f"Summary: {group['cnt']} transaction(s) flagged under rule(s): {rules}. "
        f"Maximum observed amount: {group['max_amount']} EUR.\n\n"
        f"Recommendation: Escalate to compliance officer for enhanced due diligence. "
        f"Consider temporary account monitoring and source-of-funds verification."
    )


def _openai_sar(prompt: str) -> tuple[str, str]:
    try:
        from openai import OpenAI

        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are an AML compliance analyst."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=800,
        )
        return resp.choices[0].message.content or "", OPENAI_MODEL
    except Exception as exc:
        logger.warning("OpenAI failed, using mock: %s", exc)
        return "", "mock_fallback"


def _insert_sar(conn, account_id: str, flagged_count: int, text: str, model: str) -> None:
    report_id = str(uuid.uuid4())
    account_hash = _hash_account(account_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO aml.sar_reports
                (report_id, account_id_hash, flagged_count, report_text, model_used, filed_by)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_id) DO NOTHING
            """,
            (report_id, account_hash, flagged_count, text, model, "System"),
        )
    conn.commit()
    logger.info("SAR created: %s for account hash %s", report_id, account_hash)


def run() -> int:
    conn = _connect()
    groups = _fetch_flagged_groups(conn)
    if not groups:
        logger.info("No new flagged groups for SAR generation")
        conn.close()
        return 0

    created = 0
    for group in groups:
        prompt = _build_prompt(group)
        if MOCK_MODE or not OPENAI_API_KEY:
            text = _mock_sar(group)
            model = "mock"
        else:
            text, model = _openai_sar(prompt)
            if not text:
                text = _mock_sar(group)
                model = "mock_fallback"

        _insert_sar(conn, group["account_id"], group["cnt"], text, model)
        created += 1

    conn.close()
    logger.info("Generated %d SAR report(s)", created)
    return created


def run_loop(interval_sec: int = 300) -> None:
    import time

    logger.info("SAR worker started (interval=%ds, mock=%s)", interval_sec, MOCK_MODE)
    while True:
        try:
            run()
        except Exception as exc:
            logger.exception("SAR cycle error: %s", exc)
        time.sleep(interval_sec)


if __name__ == "__main__":
    import sys

    # `once` -> single batch then exit (used by the trigger_sar Airflow DAG).
    # Default -> long-lived worker loop (used by the sar-worker container).
    if len(sys.argv) > 1 and sys.argv[1] == "once":
        run()
    else:
        run_loop()
