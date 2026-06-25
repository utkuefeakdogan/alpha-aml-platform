"""Join streaming transactions with customer master data and maintain 30d profiles."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def _read_jdbc(spark, jdbc_url: str, user: str, password: str, dbtable: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def enrich_with_customer_context(
    batch_df: DataFrame,
    spark,
    jdbc_url: str,
    pg_user: str,
    pg_password: str,
) -> DataFrame:
    """Attach customer KYC fields and primary address to each transaction row."""
    customers = _read_jdbc(
        spark,
        jdbc_url,
        pg_user,
        pg_password,
        "aml.customers",
    ).select(
        F.col("customer_id").alias("cust_id"),
        F.col("identity_no").alias("cust_identity_no"),
        F.col("name").alias("cust_name"),
        F.col("onboarding_channel").alias("cust_onboarding_channel"),
        F.col("risk_score").alias("cust_risk_score"),
        F.col("segment").alias("cust_segment"),
        F.col("customer_status").alias("cust_status"),
    )

    addresses = _read_jdbc(
        spark,
        jdbc_url,
        pg_user,
        pg_password,
        "(SELECT DISTINCT ON (customer_id) customer_id, city, district, country_code "
        "FROM aml.customer_addresses WHERE address_type = 'home' ORDER BY customer_id, address_id) addr",
    ).select(
        F.col("customer_id").alias("addr_customer_id"),
        F.col("city").alias("cust_city"),
        F.col("district").alias("cust_district"),
        F.col("country_code").alias("cust_country_code"),
    )

    sender_key = F.coalesce("sender_customer_no", "sender_id")
    enriched = (
        batch_df.withColumn("anchor_customer_id", sender_key)
        .join(customers, F.col("anchor_customer_id") == F.col("cust_id"), "left")
        .join(addresses, F.col("anchor_customer_id") == F.col("addr_customer_id"), "left")
        .drop("cust_id", "addr_customer_id")
    )
    return enriched


def compute_customer_behavior_30d(
    batch_df: DataFrame,
    spark,
    jdbc_url: str,
    pg_user: str,
    pg_password: str,
) -> DataFrame:
    """Build 30-day rolling behavior profile per customer (sender-side)."""
    batch = batch_df.withColumn(
        "customer_id",
        F.coalesce("sender_customer_no", "sender_id"),
    ).withColumn("event_ts", F.to_timestamp("ts"))

    history = _read_jdbc(
        spark,
        jdbc_url,
        pg_user,
        pg_password,
        "(SELECT COALESCE(sender_customer_no, sender_id) AS customer_id, "
        "COALESCE(receiver_customer_no, receiver_id) AS counterparty_id, "
        "amount_eur, sender_country, ts FROM aml.transactions "
        "WHERE ts >= NOW() - INTERVAL '30 days') h",
    )

    union_df = (
        batch.select(
            "customer_id",
            F.coalesce("receiver_customer_no", "receiver_id").alias("counterparty_id"),
            F.col("amount_eur").alias("amount_eur"),
            F.col("sender_country"),
            "event_ts",
        )
        .unionByName(
            history.select(
                F.col("customer_id"),
                F.col("counterparty_id"),
                F.col("amount_eur"),
                F.col("sender_country"),
                F.to_timestamp("ts").alias("event_ts"),
            ),
            allowMissingColumns=True,
        )
        .filter(F.col("customer_id").isNotNull())
    )

    start = F.expr("current_timestamp() - interval 30 days")
    windowed = union_df.filter(F.col("event_ts") >= start)

    return windowed.groupBy("customer_id").agg(
        F.count("*").alias("txn_count_30d"),
        F.sum("amount_eur").alias("volume_30d"),
        F.countDistinct("counterparty_id").alias("distinct_counterparties_30d"),
        F.first("sender_country", ignorenulls=True).alias("primary_sender_country"),
        F.max("event_ts").alias("last_txn_at"),
        F.current_timestamp().alias("computed_at"),
    )
