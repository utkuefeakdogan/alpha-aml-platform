"""Feature engineering for the AML risk models.

Features are built from the *live* operational tables (`aml.transactions`,
`aml.customers`, `aml.flagged_transactions`) over a rolling 30-day window, one
row per active sending customer. This is intentionally the same behavioral
surface the rule engine sees, so the supervised model can be benchmarked
against the rule baseline and the unsupervised model can surface near-misses
the static thresholds do not catch.

The set is bounded by *active* senders (a few thousand), not the full customer
universe, so it trains cheaply on the constrained VM.
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Model input columns (the label `flag_count_30d` / `rule_flagged` is excluded
# to avoid leakage — the supervised model must predict the alert, not read it).
FEATURE_COLUMNS: list[str] = [
    "txn_count_30d",
    "volume_30d",
    "avg_amount_30d",
    "max_txn_30d",
    "std_amount_30d",
    "distinct_receivers_30d",
    "distinct_countries_30d",
    "cross_border_txns",
    "cross_border_ratio",
    "fast_txns",
    "kyc_risk_score",
    "is_pep",
]

# Monetary / heavy-tailed columns get a log1p transform so a handful of large
# transfers do not dominate the feature space.
_LOG_COLUMNS = ("volume_30d", "avg_amount_30d", "max_txn_30d", "std_amount_30d")

_FEATURE_SQL = """
WITH txn AS (
    SELECT
        COALESCE(sender_customer_no, sender_id) AS customer_id,
        amount_eur,
        receiver_country,
        receiver_customer_no,
        txn_type
    FROM aml.transactions
    WHERE ts >= NOW() - INTERVAL '30 days'
      AND COALESCE(sender_customer_no, sender_id) IS NOT NULL
),
agg AS (
    SELECT
        customer_id,
        COUNT(*)                                   AS txn_count_30d,
        COALESCE(SUM(amount_eur), 0)               AS volume_30d,
        COALESCE(AVG(amount_eur), 0)               AS avg_amount_30d,
        COALESCE(MAX(amount_eur), 0)               AS max_txn_30d,
        COALESCE(STDDEV_POP(amount_eur), 0)        AS std_amount_30d,
        COUNT(DISTINCT receiver_customer_no)       AS distinct_receivers_30d,
        COUNT(DISTINCT receiver_country)           AS distinct_countries_30d,
        COUNT(*) FILTER (
            WHERE receiver_country IS NOT NULL AND receiver_country <> 'DE'
        )                                          AS cross_border_txns,
        COUNT(*) FILTER (WHERE txn_type = 'FAST')  AS fast_txns
    FROM txn
    GROUP BY customer_id
),
flags AS (
    SELECT customer_id, COUNT(*) AS flag_count_30d
    FROM aml.flagged_transactions
    WHERE flagged_at >= NOW() - INTERVAL '30 days'
      AND customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    a.customer_id,
    a.txn_count_30d,
    a.volume_30d,
    a.avg_amount_30d,
    a.max_txn_30d,
    a.std_amount_30d,
    a.distinct_receivers_30d,
    a.distinct_countries_30d,
    a.cross_border_txns,
    CASE WHEN a.txn_count_30d > 0
         THEN a.cross_border_txns::float / a.txn_count_30d
         ELSE 0 END                               AS cross_border_ratio,
    a.fast_txns,
    COALESCE(c.risk_score, 0)                      AS kyc_risk_score,
    CASE WHEN c.is_pep THEN 1 ELSE 0 END           AS is_pep,
    COALESCE(f.flag_count_30d, 0)                  AS flag_count_30d
FROM agg a
LEFT JOIN aml.customers c          ON c.customer_id = a.customer_id
LEFT JOIN flags f                  ON f.customer_id = a.customer_id
"""


def get_engine() -> Engine:
    """SQLAlchemy engine from the standard POSTGRES_* environment variables."""
    user = os.getenv("POSTGRES_USER", "user")
    password = os.getenv("POSTGRES_PASSWORD", "password")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "datadb")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        pool_pre_ping=True,
    )


def load_features(engine: Engine | None = None) -> pd.DataFrame:
    """Load the per-customer feature frame from live operational tables."""
    engine = engine or get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(_FEATURE_SQL), conn)
    if df.empty:
        return df
    df["rule_flagged"] = df["flag_count_30d"] > 0
    return df


def build_matrix(df: pd.DataFrame) -> np.ndarray:
    """Turn the feature frame into a numeric model matrix (with log transforms)."""
    x = df[FEATURE_COLUMNS].copy()
    for col in _LOG_COLUMNS:
        if col in x.columns:
            x[col] = np.log1p(x[col].clip(lower=0))
    return x.fillna(0.0).to_numpy(dtype="float64")
