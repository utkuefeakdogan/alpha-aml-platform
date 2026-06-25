"""Cap daily alert volume and per-rule quotas before JDBC write."""

from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

DAILY_ALERT_CAP = int(os.getenv("DAILY_ALERT_CAP", "100"))
PER_RULE_DAILY_CAP = int(os.getenv("PER_RULE_DAILY_CAP", "12"))


def _recent_flag_counts(spark, jdbc_url: str, pg_user: str, pg_password: str) -> tuple[int, dict[str, int]]:
    rows = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option(
            "dbtable",
            """
            (SELECT rule_name, COUNT(*) AS cnt
             FROM aml.flagged_transactions
             WHERE flagged_at >= NOW() - INTERVAL '24 hours'
             GROUP BY rule_name) q
            """,
        )
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .collect()
    )
    by_rule = {str(r["rule_name"]): int(r["cnt"]) for r in rows}
    return sum(by_rule.values()), by_rule


def apply_alert_budget(
    flagged: DataFrame,
    spark,
    jdbc_url: str,
    pg_user: str,
    pg_password: str,
) -> DataFrame:
    """Drop flags when global or per-rule daily caps are reached."""
    if flagged.isEmpty():
        return flagged

    total_24h, by_rule = _recent_flag_counts(spark, jdbc_url, pg_user, pg_password)
    remaining_global = max(0, DAILY_ALERT_CAP - total_24h)
    if remaining_global == 0:
        logger.info("Daily alert cap %d reached — skipping batch flags", DAILY_ALERT_CAP)
        return flagged.limit(0)

    rules = [r.rule_name for r in flagged.select("rule_name").distinct().collect()]
    allowed_rules = [
        r
        for r in rules
        if by_rule.get(r, 0) < PER_RULE_DAILY_CAP
    ]
    if not allowed_rules:
        logger.info("All rules at per-rule cap %d — skipping batch", PER_RULE_DAILY_CAP)
        return flagged.limit(0)

    filtered = flagged.filter(F.col("rule_name").isin(allowed_rules))
    if filtered.isEmpty():
        return filtered

    # Prefer rules with fewer flags today (spread across scenario types).
    priority = []
    for rule in allowed_rules:
        headroom = PER_RULE_DAILY_CAP - by_rule.get(rule, 0)
        priority.append((rule, headroom))
    priority.sort(key=lambda x: -x[1])
    ordered_rules = [r for r, _ in priority]

    parts: list[DataFrame] = []
    budget = remaining_global
    for rule in ordered_rules:
        if budget <= 0:
            break
        rule_df = filtered.filter(F.col("rule_name") == rule)
        rule_cap = min(budget, PER_RULE_DAILY_CAP - by_rule.get(rule, 0))
        parts.append(rule_df.limit(rule_cap))
        budget -= rule_cap

    if not parts:
        return flagged.limit(0)

    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out
