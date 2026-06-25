"""Archive all customer transactions when an AML alert is raised."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def _sql_quote_ids(customer_ids: list[str]) -> str:
    return ",".join("'" + c.replace("'", "''") + "'" for c in customer_ids)


def archive_alert_customer_transactions(
    flagged: DataFrame,
    batch_txns: DataFrame,
    spark,
    jdbc_url: str,
    pg_user: str,
    pg_password: str,
) -> DataFrame | None:
    """Copy every transaction for each alerted customer into the archive table."""
    if flagged.isEmpty():
        return None

    alerts = flagged.select(
        F.col("txn_id").alias("alert_txn_id"),
        "customer_id",
        "rule_name",
        "flagged_at",
    ).filter(F.col("customer_id").isNotNull())

    if alerts.isEmpty():
        return None

    customer_rows = alerts.select("customer_id").distinct().collect()
    customer_ids = [str(r.customer_id) for r in customer_rows if r.customer_id]
    if not customer_ids:
        return None

    id_sql = _sql_quote_ids(customer_ids)
    try:
        hist = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option(
                "dbtable",
                f"""
                (SELECT *
                 FROM aml.transactions
                 WHERE sender_customer_no IN ({id_sql})
                    OR receiver_customer_no IN ({id_sql})
                ) q
                """,
            )
            .option("user", pg_user)
            .option("password", pg_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
    except Exception as exc:
        logger.warning("alert archive: could not load historical txns: %s", exc)
        hist = None

    batch_slice = batch_txns.filter(
        F.col("sender_customer_no").isin(customer_ids)
        | F.col("receiver_customer_no").isin(customer_ids)
    )

    if hist is not None and not hist.isEmpty():
        all_txns = hist.unionByName(batch_slice, allowMissingColumns=True)
    else:
        all_txns = batch_slice

    if all_txns.isEmpty():
        logger.info("alert archive: no transactions found for %d customers", len(customer_ids))
        return None

    all_txns = all_txns.dropDuplicates(["txn_id"]).withColumn(
        "txn_ingested_at", F.coalesce(F.col("ingested_at"), F.current_timestamp())
    )

    a = alerts.alias("a")
    t = all_txns.alias("t")
    joined = a.join(
        t,
        (F.col("a.customer_id") == F.col("t.sender_customer_no"))
        | (F.col("a.customer_id") == F.col("t.receiver_customer_no")),
        "inner",
    )

    archive = joined.select(
        F.col("a.alert_txn_id"),
        F.col("a.rule_name"),
        F.col("a.flagged_at"),
        F.col("a.customer_id"),
        F.current_timestamp().alias("archived_at"),
        F.col("t.txn_id"),
        F.col("t.txn_category"),
        F.col("t.txn_type"),
        F.col("t.sender_id"),
        F.col("t.receiver_id"),
        F.col("t.sender_customer_no"),
        F.col("t.receiver_customer_no"),
        F.col("t.sender_name"),
        F.col("t.receiver_name"),
        F.col("t.sender_identity_no"),
        F.col("t.receiver_identity_no"),
        F.col("t.sender_branch"),
        F.col("t.receiver_branch"),
        F.col("t.sender_country"),
        F.col("t.receiver_country"),
        F.col("t.txn_description"),
        F.col("t.branch_id"),
        F.col("t.amount"),
        F.col("t.currency"),
        F.col("t.amount_eur"),
        F.col("t.country_code"),
        F.col("t.is_customer_sender"),
        F.col("t.is_customer_receiver"),
        F.col("t.is_synthetic_fraud"),
        F.col("t.fraud_type"),
        F.col("t.ts"),
        F.col("t.txn_ingested_at"),
    ).dropDuplicates(["alert_txn_id", "rule_name", "txn_id"])

    if archive.isEmpty():
        return None
    return archive
