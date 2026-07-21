"""PySpark Structured Streaming — enterprise AML with multi-window profiling."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.processing.customer_enrichment import (
    compute_customer_behavior_30d,
    enrich_with_customer_context,
)
from src.processing.alert_archive import archive_alert_customer_transactions
from src.processing.alert_budget import apply_alert_budget
from src.processing.window_engine import compute_window_metrics, evaluate_rules


def _with_alert_priority(flagged: DataFrame, spark: SparkSession) -> DataFrame:
    """Blend rule severity + amount with customer KYC risk (60/40)."""
    customers = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "(SELECT customer_id, risk_score FROM aml.customers) c")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    joined = flagged.join(customers, on="customer_id", how="left")
    rule_base = (
        F.when(F.col("rule_name") == "geographic", F.lit(90.0))
        .when(F.col("rule_name") == "high_value", F.lit(85.0))
        .when(F.col("rule_name") == "smurfing", F.lit(80.0))
        .when(F.col("rule_name") == "weekly_volume", F.lit(70.0))
        .when(F.col("rule_name") == "daily_velocity", F.lit(65.0))
        .when(F.col("rule_name") == "monthly_peer_anomaly", F.lit(60.0))
        .when(F.col("rule_name") == "dormant_reactivation", F.lit(75.0))
        .when(F.col("rule_name") == "mule_inbound", F.lit(82.0))
        .otherwise(F.lit(50.0))
    )
    amount_bonus = F.least(F.coalesce(F.col("amount_eur"), F.col("amount"), F.lit(0.0)) / 500.0, F.lit(10.0))
    rule_signal = F.least(rule_base + amount_bonus, F.lit(100.0))
    priority = F.round(F.lit(0.6) * rule_signal + F.lit(0.4) * F.coalesce(F.col("risk_score"), F.lit(0.0)), 2)
    return joined.withColumn("alert_priority_score", priority).drop("risk_score")

logging.basicConfig(level=logging.INFO)
try:
    from src.common.event_log import install_pg_log_handler

    install_pg_log_handler("spark")
except Exception:  # pragma: no cover - log mirroring is best-effort
    pass
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "transactions.raw")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints")
RULES_JSON_PATH = os.getenv("RULES_JSON_PATH", "/app/configs/rules.json")
RULES_YAML_PATH = os.getenv("RULES_PATH", "/app/configs/rules.yaml")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "datadb")
PG_USER = os.getenv("POSTGRES_USER", "user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

TXN_SCHEMA = StructType(
    [
        StructField("txn_id", StringType()),
        StructField("txn_category", StringType()),
        StructField("txn_type", StringType()),
        StructField("sender_id", StringType()),
        StructField("receiver_id", StringType()),
        StructField("sender_customer_no", StringType()),
        StructField("receiver_customer_no", StringType()),
        StructField("sender_name", StringType()),
        StructField("receiver_name", StringType()),
        StructField("sender_identity_no", StringType()),
        StructField("receiver_identity_no", StringType()),
        StructField("sender_branch", StringType()),
        StructField("receiver_branch", StringType()),
        StructField("sender_country", StringType()),
        StructField("receiver_country", StringType()),
        StructField("txn_description", StringType()),
        StructField("branch_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("amount_eur", DoubleType()),
        StructField("country_code", StringType()),
        StructField("is_customer_sender", BooleanType()),
        StructField("is_customer_receiver", BooleanType()),
        StructField("ts", StringType()),
        StructField("is_synthetic_fraud", BooleanType()),
        StructField("fraud_type", StringType()),
        StructField("account_id", StringType()),
        StructField("merchant", StringType()),
    ]
)


def load_rules() -> dict:
    json_path = Path(RULES_JSON_PATH)
    if json_path.exists():
        with open(json_path) as f:
            return json.load(f)
    yaml_path = Path(RULES_YAML_PATH)
    if yaml_path.exists():
        with open(yaml_path) as f:
            return yaml.safe_load(f)
    raise FileNotFoundError("No rules file found")


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("alpha-aml-enterprise")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "512m"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "512m"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def _write_jdbc(df, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def _write_metrics_upsert(metrics_df) -> None:
    """Upsert window metrics so 30s batches update rows instead of appending 121M+ rows."""
    import psycopg2
    from psycopg2.extras import execute_batch

    rows = metrics_df.collect()
    if not rows:
        return

    sql = """
        INSERT INTO aml.account_window_metrics (
            customer_id, window_type, total_volume, txn_count,
            distinct_receiver_count, window_start, computed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id, window_type, window_start)
        DO UPDATE SET
            total_volume = EXCLUDED.total_volume,
            txn_count = EXCLUDED.txn_count,
            distinct_receiver_count = EXCLUDED.distinct_receiver_count,
            computed_at = EXCLUDED.computed_at
    """
    payload = [
        (
            r.customer_id,
            r.window_type,
            float(r.total_volume or 0),
            int(r.txn_count or 0),
            int(r.distinct_receiver_count or 0),
            r.window_start,
            r.computed_at,
        )
        for r in rows
    ]
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            execute_batch(cur, sql, payload, page_size=500)
        conn.commit()
    finally:
        conn.close()


def _fx_to_eur_col():
    return (
        F.when(F.col("currency") == "USD", F.col("amount") * F.lit(0.92))
        .when(F.col("currency") == "TRY", F.col("amount") * F.lit(0.027))
        .when(F.col("currency") == "GBP", F.col("amount") * F.lit(1.17))
        .when(F.col("currency") == "CHF", F.col("amount") * F.lit(1.05))
        .otherwise(F.col("amount"))
    )


def _normalize_batch(batch_df):
    df = batch_df.withColumn(
        "sender_customer_no",
        F.coalesce("sender_customer_no", "sender_id", "account_id"),
    ).withColumn(
        "sender_id",
        F.col("sender_customer_no"),
    ).withColumn(
        "receiver_customer_no",
        F.coalesce("receiver_customer_no", "receiver_id"),
    ).withColumn(
        "receiver_id",
        F.coalesce("receiver_customer_no", "receiver_id"),
    ).withColumn(
        "txn_category", F.coalesce("txn_category", F.lit("Wire")),
    ).withColumn(
        "txn_type", F.coalesce("txn_type", F.lit("FAST")),
    ).withColumn(
        "branch_id", F.coalesce("branch_id", "sender_branch", F.lit("BR-001")),
    ).withColumn(
        "sender_branch", F.coalesce("sender_branch", "branch_id"),
    ).withColumn(
        "country_code",
        F.coalesce("country_code", "sender_country", "receiver_country", F.lit("DE")),
    ).withColumn(
        "currency", F.coalesce("currency", F.lit("EUR")),
    ).withColumn(
        "amount_eur",
        F.coalesce("amount_eur", _fx_to_eur_col()),
    )
    return df


def process_batch(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    try:
        rules = load_rules()
    except Exception as exc:
        logger.error("Rules load failed: %s", exc)
        return

    df = _normalize_batch(batch_df)
    spark = df.sparkSession

    try:
        enriched = enrich_with_customer_context(df, spark, JDBC_URL, PG_USER, PG_PASSWORD)
        behavior = compute_customer_behavior_30d(df, spark, JDBC_URL, PG_USER, PG_PASSWORD)
        if behavior is not None and not behavior.isEmpty():
            (
                behavior.write.format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", "aml.customer_behavior_30d")
                .option("user", PG_USER)
                .option("password", PG_PASSWORD)
                .option("driver", "org.postgresql.Driver")
                .option("truncate", "true")
                .mode("overwrite")
                .save()
            )
        logger.debug("Enriched batch rows: %d", enriched.count())
    except Exception as exc:
        logger.warning("Customer enrichment skipped batch %d: %s", batch_id, exc)

    txn_write = df.select(
        "txn_id", "txn_category", "txn_type",
        "sender_id", "receiver_id",
        "sender_customer_no", "receiver_customer_no",
        "sender_name", "receiver_name",
        "sender_identity_no", "receiver_identity_no",
        "sender_branch", "receiver_branch",
        "sender_country", "receiver_country", "txn_description",
        "branch_id", "amount", "currency", "amount_eur",
        F.coalesce("country_code", "sender_country", "receiver_country", F.lit("DE")).alias("country_code"),
        F.coalesce("is_customer_sender", F.lit(True)).alias("is_customer_sender"),
        F.coalesce("is_customer_receiver", F.lit(False)).alias("is_customer_receiver"),
        F.coalesce("is_synthetic_fraud", F.lit(False)).alias("is_synthetic_fraud"),
        "fraud_type",
        F.to_timestamp("ts").alias("ts"),
        F.current_timestamp().alias("ingested_at"),
    )

    try:
        _write_jdbc(txn_write, "aml.transactions")
    except Exception as exc:
        logger.error("Transaction write failed batch %d: %s", batch_id, exc)

    metrics = None
    try:
        metrics = compute_window_metrics(df, spark, JDBC_URL, PG_USER, PG_PASSWORD)
        if metrics is not None and not metrics.isEmpty():
            metrics_write = (
                metrics.filter(F.col("sender_id").isNotNull())
                .select(
                    F.col("sender_id").alias("customer_id"),
                    "window_type",
                    "total_volume",
                    "txn_count",
                    "distinct_receiver_count",
                    "window_start",
                    "computed_at",
                )
            )
            if not metrics_write.isEmpty():
                _write_metrics_upsert(metrics_write)
        flagged = evaluate_rules(df, metrics, rules, spark, JDBC_URL, PG_USER, PG_PASSWORD)
        if not flagged.isEmpty():
            flagged = apply_alert_budget(flagged, spark, JDBC_URL, PG_USER, PG_PASSWORD)
            if not flagged.isEmpty():
                flagged = _with_alert_priority(flagged, spark)
                n = flagged.count()
                _write_jdbc(flagged, "aml.flagged_transactions")
                archive_df = archive_alert_customer_transactions(
                    flagged, txn_write, spark, JDBC_URL, PG_USER, PG_PASSWORD
                )
                if archive_df is not None and not archive_df.isEmpty():
                    arch_n = archive_df.count()
                    _write_jdbc(archive_df, "aml.alert_transaction_archive")
                    logger.info(
                        "Batch %d: flagged %d, archived %d txn snapshots",
                        batch_id,
                        n,
                        arch_n,
                    )
                else:
                    logger.info("Batch %d: flagged %d (after budget)", batch_id, n)
    except Exception as exc:
        logger.error("Metrics/flagging failed batch %d: %s", batch_id, exc)


def run() -> None:
    rules = load_rules()
    logger.info("Enterprise streaming rules loaded: %s", list(rules.keys()))
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        kafka_df.select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("data"))
        .select("data.*")
        .filter(F.col("txn_id").isNotNull())
    )

    query = (
        parsed.writeStream.foreachBatch(lambda df, bid: process_batch(df, bid))
        .option("checkpointLocation", CHECKPOINT_DIR + "-enterprise-v4")
        .trigger(processingTime="30 seconds")
        .start()
    )
    logger.info("Enterprise streaming started")
    query.awaitTermination()


if __name__ == "__main__":
    try:
        run()
    except Exception:
        logger.exception("Streaming failed")
        sys.exit(1)
