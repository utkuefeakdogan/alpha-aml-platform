"""Context-aware SAR intelligence (template-based, no external API)."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone

from src.dashboard.db import fetch_sar_context, insert_sar_report


def hash_customer(customer_id: str) -> str:
    return hashlib.sha256(customer_id.encode()).hexdigest()[:16]


def build_intelligence_summary(customer_id: str, rule_name: str, txn_id: str) -> str:
    ctx = fetch_sar_context(customer_id)
    if not ctx:
        return f"No context available for {customer_id}."

    weekly_vol = ctx["weekly_volume"]
    weekly_max = 10000  # default; dashboard rules may differ
    pct_over = ((weekly_vol - weekly_max) / weekly_max * 100) if weekly_max else 0

    narrative_parts = []
    if rule_name == "weekly_volume" and weekly_vol > weekly_max:
        narrative_parts.append(
            f"Customer {customer_id} exceeded weekly volume by {pct_over:.0f}% "
            f"({weekly_vol:,.2f} EUR vs threshold {weekly_max:,.0f} EUR)."
        )
    if ctx["weekly_fast_count"] >= 10:
        narrative_parts.append(
            f"Observed {ctx['weekly_fast_count']} FAST transfers in 7 days "
            f"(potential smurfing pattern)."
        )
    if rule_name == "daily_velocity":
        narrative_parts.append(
            f"Daily velocity rule triggered on alert {txn_id}."
        )
    if rule_name == "geographic":
        narrative_parts.append("Transaction involves high-risk jurisdiction.")
    if not narrative_parts:
        narrative_parts.append(
            f"Customer {customer_id} flagged under rule '{rule_name}' on txn {txn_id}."
        )

    return (
        f"SAR INTELLIGENCE BRIEF\n"
        f"Generated: {datetime.now(timezone.utc).isoformat()}\n"
        f"Customer: {ctx['name']} ({customer_id})\n"
        f"Segment: {ctx['segment']} | Risk score: {ctx['risk_score']}\n"
        f"30-day volume: {ctx['volume_30d']:,.2f} EUR across {ctx['txn_count_30d']} txns\n\n"
        f"NARRATIVE\n"
        + "\n".join(f"- {n}" for n in narrative_parts)
        + f"\n\nLineage: trace txn_id {txn_id} in aml.transactions → aml.flagged_transactions."
    )


def create_sar_from_context(
    customer_id: str,
    txn_id: str,
    rule_name: str,
    rule_detail: str,
    analyst_name: str,
) -> tuple[str, str]:
    report_id = str(uuid.uuid4())
    filed_at = datetime.now(timezone.utc).isoformat()
    text = build_intelligence_summary(customer_id, rule_name, txn_id)
    text = (
        f"Filed by: {analyst_name}\nFiled at: {filed_at}\n\n" + text
    )
    text += f"\n\nFULL REPORT\n{'='*40}\n"
    text += (
        f"Rule: {rule_name}\nDetail: {rule_detail}\n"
        f"Subject hash: {hash_customer(customer_id)}\n"
        f"Recommendation: Escalate to compliance officer for EDD review."
    )
    ctx = fetch_sar_context(customer_id)
    flag_cnt = max(int(ctx.get("txn_count_30d", 1)), 1)
    insert_sar_report(
        report_id, hash_customer(customer_id), flag_cnt, text, customer_id,
        "sar_intelligence", filed_by=analyst_name,
    )
    return report_id, text
