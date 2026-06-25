"""Multi-window aggregation and rule evaluation for AML streaming."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def _scenario_only(df: DataFrame) -> DataFrame:
    """Only scenario-driven transactions should drive most AML flags in demo mode."""
    return df.filter(F.coalesce(F.col("is_synthetic_fraud"), F.lit(False)))


def _one_flag_per_customer(df: DataFrame) -> DataFrame:
    """At most one flagged row per customer_id per rule batch."""
    if df.isEmpty():
        return df
    w = Window.partitionBy("customer_id", "rule_name").orderBy(F.col("event_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

WINDOWS = {
    "daily": timedelta(days=1),
    "weekly": timedelta(days=7),
    "biweekly": timedelta(days=14),
    "monthly": timedelta(days=30),
}


def _window_start(now: datetime, window_type: str, delta: timedelta) -> datetime:
    """Align window boundaries so upserts deduplicate instead of sliding every batch."""
    ts = now.replace(minute=0, second=0, microsecond=0)
    if window_type == "daily":
        return ts.replace(hour=0)
    if window_type == "weekly":
        start = ts.replace(hour=0)
        return start - timedelta(days=start.weekday())
    if window_type == "biweekly":
        start = ts.replace(hour=0)
        epoch = datetime(2026, 1, 1, tzinfo=timezone.utc)
        days = (start - epoch).days
        period_start = days - (days % 14)
        return epoch + timedelta(days=period_start)
    if window_type == "monthly":
        return ts.replace(day=1, hour=0)
    return now - delta


def compute_window_metrics(batch_df: DataFrame, spark, jdbc_url: str, pg_user: str, pg_password: str) -> DataFrame:
    """Aggregate metrics per sender across four window types using batch + PG history."""
    now = datetime.now(timezone.utc)
    batch = batch_df.withColumn(
        "sender_id",
        F.coalesce("sender_customer_no", "sender_id"),
    ).withColumn("event_ts", F.to_timestamp("ts"))

    history = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option(
            "dbtable",
            "(SELECT COALESCE(sender_customer_no, sender_id) AS sender_id, "
            "COALESCE(receiver_customer_no, receiver_id) AS receiver_id, "
            "amount_eur, ts FROM aml.transactions "
            "WHERE ts >= NOW() - INTERVAL '30 days') h",
        )
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    union_df = (
        batch.select(
            F.coalesce("sender_customer_no", "sender_id").alias("sender_id"),
            F.coalesce("receiver_customer_no", "receiver_id").alias("receiver_id"),
            F.col("amount_eur").alias("amount_eur"),
            "event_ts",
        )
        .unionByName(
            history.select(
                F.col("sender_id"),
                F.col("receiver_id"),
                F.col("amount_eur"),
                F.to_timestamp("ts").alias("event_ts"),
            ),
            allowMissingColumns=True,
        )
        .dropDuplicates(["sender_id", "receiver_id", "amount_eur", "event_ts"])
    )

    metrics_rows = []
    for window_type, delta in WINDOWS.items():
        start = _window_start(now, window_type, delta)
        wdf = union_df.filter(F.col("event_ts") >= F.lit(start)).filter(
            F.col("sender_id").isNotNull()
        )
        agg = wdf.groupBy("sender_id").agg(
            F.sum("amount_eur").alias("total_volume"),
            F.count("*").alias("txn_count"),
            F.countDistinct("receiver_id").alias("distinct_receiver_count"),
        )
        metrics_rows.append(
            agg.withColumn("window_type", F.lit(window_type)).withColumn(
                "window_start", F.lit(start)
            )
        )

    if not metrics_rows:
        return batch_df.limit(0)

    result = metrics_rows[0]
    for m in metrics_rows[1:]:
        result = result.unionByName(m)
    return result.withColumn("computed_at", F.current_timestamp())


def evaluate_rules(
    batch_df: DataFrame,
    metrics_df: DataFrame | None,
    rules: dict,
    spark=None,
    jdbc_url: str | None = None,
    pg_user: str | None = None,
    pg_password: str | None = None,
) -> DataFrame:
    """Return flagged transaction rows based on multi-window rules."""
    mw = rules.get("multi_window", {})
    daily_max = int(mw.get("daily_velocity_max", 50))
    daily_amount_cap = float(mw.get("daily_velocity_max_amount_eur", 1000))
    weekly_vol = float(mw.get("weekly_volume_max_eur", 10000))
    monthly_mult = float(mw.get("monthly_peer_anomaly_multiplier", 2.5))
    monthly_base = int(mw.get("monthly_peer_baseline_txn_count", 30))
    blocked = rules.get("geographic", {}).get("blocked_countries", [])
    high_risk = rules.get("geographic", {}).get("high_risk_countries", ["RU", "KP"])
    threshold = float(rules.get("high_value", {}).get("threshold_eur", 10000))

    base = (
        batch_df.withColumn("event_ts", F.to_timestamp("ts"))
        .withColumn("sender_id", F.coalesce("sender_customer_no", "sender_id"))
        .withColumn("amount_eur", F.coalesce("amount_eur", F.col("amount")))
        .withColumn(
            "customer_id",
            F.when(
                F.col("is_customer_receiver") & F.col("receiver_customer_no").isNotNull(),
                F.col("receiver_customer_no"),
            ).otherwise(F.coalesce("sender_customer_no", "sender_id")),
        )
    )
    smurf_threshold = float(rules.get("smurfing", {}).get("weekly_small_txn_threshold_eur", 500))
    smurf_count = int(rules.get("smurfing", {}).get("weekly_small_txn_count", 15))
    dormant_min = float(rules.get("dormant_reactivation", {}).get("min_amount_eur", 3000))
    mule_min_senders = int(rules.get("mule_inbound", {}).get("min_distinct_senders", 5))

    flags: list = []
    scenario_base = _scenario_only(base)

    if metrics_df is not None and not metrics_df.isEmpty():
        # Weekly volume
        weekly = metrics_df.filter(F.col("window_type") == "weekly").filter(
            F.col("total_volume") > weekly_vol
        )
        if not weekly.isEmpty():
            flagged = scenario_base.join(weekly, scenario_base.customer_id == weekly.sender_id).select(
                scenario_base.txn_id,
                scenario_base.customer_id,
                scenario_base.sender_id.alias("account_id"),
                scenario_base.amount,
                scenario_base.currency,
                scenario_base.amount_eur,
                scenario_base.event_ts,
                F.lit("weekly_volume").alias("rule_name"),
                F.concat(
                    F.lit("weekly volume "),
                    weekly.total_volume.cast("string"),
                    F.lit(f" EUR exceeds {weekly_vol}"),
                ).alias("rule_detail"),
                F.lit("weekly").alias("window_type"),
                F.current_timestamp().alias("flagged_at"),
            )
            flags.append(_one_flag_per_customer(flagged))

        # Daily velocity: high count of small-value rapid transfers (under-radar pattern)
        daily = metrics_df.filter(F.col("window_type") == "daily").filter(
            F.col("txn_count") > daily_max
        )
        daily_small = scenario_base.filter(F.col("amount_eur") <= daily_amount_cap)
        if not daily.isEmpty() and not daily_small.isEmpty():
            flagged = daily_small.join(daily, daily_small.customer_id == daily.sender_id).select(
                daily_small.txn_id,
                daily_small.customer_id,
                daily_small.sender_id.alias("account_id"),
                daily_small.amount,
                daily_small.currency,
                daily_small.amount_eur,
                daily_small.event_ts,
                F.lit("daily_velocity").alias("rule_name"),
                F.concat(
                    F.lit("daily txn_count "),
                    daily.txn_count.cast("string"),
                    F.lit(f" exceeds {daily_max} (txns <= {daily_amount_cap} EUR)"),
                ).alias("rule_detail"),
                F.lit("daily").alias("window_type"),
                F.current_timestamp().alias("flagged_at"),
            )
            flags.append(_one_flag_per_customer(flagged))

        # Monthly peer anomaly
        monthly = metrics_df.filter(F.col("window_type") == "monthly").filter(
            F.col("txn_count") > monthly_base * monthly_mult
        )
        if not monthly.isEmpty():
            flagged = scenario_base.join(monthly, scenario_base.customer_id == monthly.sender_id).select(
                scenario_base.txn_id,
                scenario_base.customer_id,
                scenario_base.sender_id.alias("account_id"),
                scenario_base.amount,
                scenario_base.currency,
                scenario_base.amount_eur,
                scenario_base.event_ts,
                F.lit("monthly_peer_anomaly").alias("rule_name"),
                F.concat(
                    F.lit("monthly txn_count "),
                    monthly.txn_count.cast("string"),
                    F.lit(f" exceeds peer baseline x{monthly_mult}"),
                ).alias("rule_detail"),
                F.lit("monthly").alias("window_type"),
                F.current_timestamp().alias("flagged_at"),
            )
            flags.append(_one_flag_per_customer(flagged))

    # Geographic high-risk on current batch (sender or receiver country)
    risk_country = F.coalesce("sender_country", "receiver_country", "country_code")
    geo = scenario_base.filter(risk_country.isin(high_risk + blocked))
    if not geo.isEmpty():
        flags.append(
            _one_flag_per_customer(
                geo.select(
                    "txn_id",
                    "customer_id",
                    F.col("sender_id").alias("account_id"),
                    "amount",
                    "currency",
                    "amount_eur",
                    "event_ts",
                    F.lit("geographic").alias("rule_name"),
                    F.concat(
                        F.lit("high-risk country: "),
                        risk_country,
                    ).alias("rule_detail"),
                    F.lit("daily").alias("window_type"),
                    F.current_timestamp().alias("flagged_at"),
                )
            )
        )

    # High value single txn (scenario only — normal traffic capped below threshold)
    hv = scenario_base.filter(F.col("amount_eur") > threshold)
    if not hv.isEmpty():
        flags.append(
            _one_flag_per_customer(
            hv.select(
                "txn_id",
                "customer_id",
                F.col("sender_id").alias("account_id"),
                "amount",
                "currency",
                "amount_eur",
                "event_ts",
                F.lit("high_value").alias("rule_name"),
                F.lit(f"amount_eur exceeds {threshold} EUR").alias("rule_detail"),
                F.lit("daily").alias("window_type"),
                F.current_timestamp().alias("flagged_at"),
            )
            )
        )

    # Smurfing: small txns in batch + elevated weekly txn count
    if metrics_df is not None and not metrics_df.isEmpty():
        weekly_smurf = metrics_df.filter(F.col("window_type") == "weekly").filter(
            F.col("txn_count") >= smurf_count
        )
        small_batch = scenario_base.filter(F.col("amount_eur") <= smurf_threshold)
        if not small_batch.isEmpty() and not weekly_smurf.isEmpty():
            flagged = small_batch.join(
                weekly_smurf, small_batch.customer_id == weekly_smurf.sender_id
            ).select(
                small_batch.txn_id,
                small_batch.customer_id,
                small_batch.sender_id.alias("account_id"),
                small_batch.amount,
                small_batch.currency,
                small_batch.amount_eur,
                small_batch.event_ts,
                F.lit("smurfing").alias("rule_name"),
                F.lit(
                    f"weekly activity >= {smurf_count} with txns under {smurf_threshold} EUR"
                ).alias("rule_detail"),
                F.lit("weekly").alias("window_type"),
                F.current_timestamp().alias("flagged_at"),
            )
            flags.append(_one_flag_per_customer(flagged))

    # Dormant reactivation: dormant customer + large txn
    dormant_txns = scenario_base.filter(F.col("amount_eur") >= dormant_min)
    if not dormant_txns.isEmpty() and spark and jdbc_url:
        try:
            customers = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", "(SELECT customer_id, customer_status FROM aml.customers) c")
                .option("user", pg_user)
                .option("password", pg_password)
                .option("driver", "org.postgresql.Driver")
                .load()
            )
            dormant_ids = customers.filter(F.col("customer_status") == "dormant").select(
                "customer_id"
            )
            dormant_flag = dormant_txns.join(dormant_ids, "customer_id").select(
                "txn_id",
                "customer_id",
                F.col("sender_id").alias("account_id"),
                "amount",
                "currency",
                "amount_eur",
                "event_ts",
                F.lit("dormant_reactivation").alias("rule_name"),
                F.lit(f"dormant account activity >= {dormant_min} EUR").alias("rule_detail"),
                F.lit("daily").alias("window_type"),
                F.current_timestamp().alias("flagged_at"),
            )
            if not dormant_flag.isEmpty():
                flags.append(dormant_flag)
        except Exception as exc:
            logger.warning("dormant_reactivation rule skipped: %s", exc)

    # Mule inbound: many distinct external senders to same receiver in batch
    inbound = scenario_base.filter(
        F.col("is_customer_receiver")
        & F.col("receiver_customer_no").isNotNull()
        & F.col("sender_customer_no").isNull()
    )
    if not inbound.isEmpty():
        mule_agg = inbound.groupBy("receiver_customer_no").agg(
            F.countDistinct("sender_identity_no").alias("distinct_senders"),
        )
        mule_receivers = mule_agg.filter(
            F.col("distinct_senders") >= mule_min_senders
        ).select(F.col("receiver_customer_no").alias("mule_receiver_id"))
        if not mule_receivers.isEmpty():
            flagged = inbound.join(
                mule_receivers,
                inbound.receiver_customer_no == mule_receivers.mule_receiver_id,
            ).select(
                inbound.txn_id,
                inbound.receiver_customer_no.alias("customer_id"),
                inbound.receiver_customer_no.alias("account_id"),
                inbound.amount,
                inbound.currency,
                inbound.amount_eur,
                inbound.event_ts,
                F.lit("mule_inbound").alias("rule_name"),
                F.lit(f"distinct inbound senders >= {mule_min_senders}").alias("rule_detail"),
                F.lit("daily").alias("window_type"),
                F.current_timestamp().alias("flagged_at"),
            )
            flags.append(_one_flag_per_customer(flagged))

    if not flags:
        return base.limit(0).select(
            "txn_id",
            "customer_id",
            F.col("sender_id").alias("account_id"),
            "amount",
            "currency",
            "amount_eur",
            F.col("event_ts").alias("ts"),
            F.lit(None).cast("string").alias("merchant"),
            F.lit(None).cast("string").alias("rule_name"),
            F.lit(None).cast("string").alias("rule_detail"),
            F.lit(None).cast("string").alias("window_type"),
            F.current_timestamp().alias("flagged_at"),
        )

    result = flags[0]
    for f in flags[1:]:
        result = result.unionByName(f, allowMissingColumns=True)

    return result.filter(F.col("customer_id").isNotNull()).withColumn(
        "account_id", F.coalesce(F.col("account_id"), F.col("customer_id"))
    ).select(
        "txn_id",
        "customer_id",
        "account_id",
        "amount",
        "currency",
        "amount_eur",
        F.col("event_ts").alias("ts"),
        F.lit(None).cast("string").alias("merchant"),
        "rule_name",
        "rule_detail",
        "window_type",
        "flagged_at",
    ).dropDuplicates(["txn_id", "rule_name"])
