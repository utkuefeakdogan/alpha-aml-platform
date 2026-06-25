-- Airflow pipeline audit metrics (hourly snapshots).
CREATE TABLE IF NOT EXISTS aml.pipeline_metrics (
    id              SERIAL PRIMARY KEY,
    metric_key      VARCHAR(64) NOT NULL,
    metric_value    BIGINT NOT NULL,
    metric_detail   TEXT,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_key_time
    ON aml.pipeline_metrics (metric_key, recorded_at DESC);
