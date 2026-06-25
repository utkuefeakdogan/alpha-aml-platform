-- ML risk layer: per-customer model scores + model-run metadata/metrics.
--
-- Two tables:
--   aml.ml_customer_scores  current snapshot of behavioral risk scores
--                           (unsupervised anomaly + supervised triage),
--                           one row per customer for the latest model run.
--   aml.ml_model_runs       append-only registry of training runs with
--                           evaluation metrics (ROC/PR), curve points and
--                           feature importances so the dashboard can render
--                           model quality without recomputing.

CREATE TABLE IF NOT EXISTS aml.ml_customer_scores (
    customer_id             VARCHAR(32) NOT NULL,
    model_version           VARCHAR(64) NOT NULL,
    anomaly_score           NUMERIC(8, 5) NOT NULL,   -- normalized 0..1, higher = more anomalous
    anomaly_rank            INTEGER,
    is_anomaly              BOOLEAN NOT NULL DEFAULT FALSE,
    triage_score            NUMERIC(8, 5),            -- supervised P(alert), NULL if not trained
    rule_flagged            BOOLEAN NOT NULL DEFAULT FALSE,
    txn_count_30d           INTEGER,
    volume_30d              NUMERIC(18, 2),
    distinct_receivers_30d  INTEGER,
    max_txn_30d             NUMERIC(18, 2),
    kyc_risk_score          NUMERIC(5, 2),
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (customer_id, model_version)
);

CREATE INDEX IF NOT EXISTS idx_ml_scores_anomaly
    ON aml.ml_customer_scores (model_version, anomaly_score DESC);
CREATE INDEX IF NOT EXISTS idx_ml_scores_triage
    ON aml.ml_customer_scores (model_version, triage_score DESC);

CREATE TABLE IF NOT EXISTS aml.ml_model_runs (
    run_id              BIGSERIAL PRIMARY KEY,
    model_version       VARCHAR(64) NOT NULL,
    trained_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    n_samples           INTEGER NOT NULL,
    n_features          INTEGER NOT NULL,
    n_anomalies         INTEGER,
    contamination       NUMERIC(6, 4),
    supervised_trained  BOOLEAN NOT NULL DEFAULT FALSE,
    positive_rate       NUMERIC(6, 4),
    roc_auc             NUMERIC(6, 4),
    pr_auc              NUMERIC(6, 4),
    precision_score     NUMERIC(6, 4),
    recall_score        NUMERIC(6, 4),
    f1_score            NUMERIC(6, 4),
    roc_curve           JSONB,    -- {"fpr": [...], "tpr": [...]}
    pr_curve            JSONB,    -- {"recall": [...], "precision": [...]}
    feature_importance  JSONB,    -- {"feature": importance, ...}
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_ml_runs_trained_at
    ON aml.ml_model_runs (trained_at DESC);

-- Expose to the read-only SQL Explorer role (consistent with other aml tables).
GRANT SELECT ON aml.ml_customer_scores TO freesql_reader;
GRANT SELECT ON aml.ml_model_runs TO freesql_reader;
