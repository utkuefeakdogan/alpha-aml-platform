"""Export Postgres Gold tables to BigQuery via GCS Parquet.

Flow (one table at a time):
  SELECT * FROM gold.<table>
    → write Parquet to a temp file
    → upload to gs://<bucket>/aml_gold/<table>/<ts>.parquet
    → BigQuery load (WRITE_TRUNCATE) into <project>.aml_analytics.<table>

Designed for the portfolio "export bridge" — not a dual dbt target.
Requires GOOGLE_APPLICATION_CREDENTIALS (or ADC) with least-privilege
access to the staging bucket and the BigQuery dataset.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from google.cloud import bigquery, storage
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

GOLD_TABLES: tuple[str, ...] = (
    "gold_daily_fraud_summary",
    "gold_account_risk_score",
    "gold_customer_risk_profile",
)

# dbt custom schema on Postgres becomes {target_schema}_{custom} → aml_gold
GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "aml_gold")
# Staging Parquet retention (Object Admin can delete objects; bucket lifecycle
# needs stronger IAM — so we prune in-process after each successful sync).
GCS_RETENTION_DAYS = int(os.getenv("GCS_RETENTION_DAYS", "7"))


def _pg_engine():
    user = os.getenv("POSTGRES_USER", "user")
    password = os.getenv("POSTGRES_PASSWORD", "password")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "datadb")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        pool_pre_ping=True,
    )


def _require_env(*keys: str) -> dict[str, str]:
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError(
            "BigQuery export is not configured. Set env vars: "
            + ", ".join(missing)
            + ". See .env.example (GCP_PROJECT / GCS_BUCKET / BQ_DATASET)."
        )
    return {k: os.environ[k] for k in keys}


def _ensure_dataset(bq: bigquery.Client, project: str, dataset_id: str, location: str) -> None:
    ds_ref = bigquery.Dataset(f"{project}.{dataset_id}")
    ds_ref.location = location
    bq.create_dataset(ds_ref, exists_ok=True)
    logger.info("BigQuery dataset ready: %s.%s (%s)", project, dataset_id, location)


def _export_one(
    *,
    engine,
    bq: bigquery.Client,
    gcs: storage.Client,
    project: str,
    dataset_id: str,
    bucket_name: str,
    table: str,
    run_id: str,
) -> dict:
    sql = text(f'SELECT * FROM {GOLD_SCHEMA}."{table}"')
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    blob_path = f"aml_gold/{table}/{run_id}.parquet"
    gcs_uri = f"gs://{bucket_name}/{blob_path}"

    with tempfile.TemporaryDirectory(prefix="bq_export_") as tmp:
        local = Path(tmp) / f"{table}.parquet"
        df.to_parquet(local, index=False)
        bucket = gcs.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(str(local))
        logger.info("Uploaded %s rows → %s", len(df), gcs_uri)

    table_id = f"{project}.{dataset_id}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    load_job = bq.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
    dest = bq.get_table(table_id)
    logger.info("Loaded %s → %s (%s rows)", gcs_uri, table_id, dest.num_rows)
    return {
        "table": table,
        "rows": int(dest.num_rows or 0),
        "gcs_uri": gcs_uri,
        "bq_table": table_id,
    }


def _prune_gcs_staging(
    gcs: storage.Client,
    bucket_name: str,
    *,
    retention_days: int = GCS_RETENTION_DAYS,
    prefix: str = "aml_gold/",
) -> dict:
    """Delete staging Parquet objects older than retention_days.

    Uses object-level delete (works with roles/storage.objectAdmin). A bucket
    lifecycle rule is nicer long-term and is managed in Terraform when IAM
    allows storage.buckets.update.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    bucket = gcs.bucket(bucket_name)
    deleted = 0
    kept = 0
    for blob in bucket.list_blobs(prefix=prefix):
        updated = blob.updated
        if updated is not None and updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        if updated is not None and updated < cutoff:
            blob.delete()
            deleted += 1
            logger.info("Pruned stale staging object %s (updated %s)", blob.name, updated)
        else:
            kept += 1
    summary = {
        "prefix": prefix,
        "retention_days": retention_days,
        "deleted": deleted,
        "kept": kept,
    }
    logger.info("GCS staging prune: %s", summary)
    return summary


def run_export(tables: tuple[str, ...] | None = None) -> dict:
    """Export Gold tables. Raises with a clear message if credentials/env missing."""
    cfg = _require_env("GCP_PROJECT", "GCS_BUCKET", "BQ_DATASET")
    project = cfg["GCP_PROJECT"]
    bucket_name = cfg["GCS_BUCKET"]
    dataset_id = cfg["BQ_DATASET"]
    location = os.getenv("BQ_LOCATION", "EU")
    tables = tables or GOLD_TABLES
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if not (
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or os.getenv("GOOGLE_CLOUD_PROJECT")
    ):
        # ADC can still work via metadata; warn if neither path nor project hint.
        logger.info(
            "GOOGLE_APPLICATION_CREDENTIALS unset — relying on Application Default Credentials"
        )

    engine = _pg_engine()
    bq = bigquery.Client(project=project)
    gcs = storage.Client(project=project)
    _ensure_dataset(bq, project, dataset_id, location)

    results = []
    for table in tables:
        results.append(
            _export_one(
                engine=engine,
                bq=bq,
                gcs=gcs,
                project=project,
                dataset_id=dataset_id,
                bucket_name=bucket_name,
                table=table,
                run_id=run_id,
            )
        )

    prune = _prune_gcs_staging(gcs, bucket_name)

    summary = {
        "run_id": run_id,
        "project": project,
        "dataset": dataset_id,
        "bucket": bucket_name,
        "tables": results,
        "gcs_prune": prune,
        "status": "ok",
    }
    logger.info("Gold → BigQuery export complete: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run_export())
