-- Central application event/error log.
--
-- aml.event_log collects WARNING+ records emitted by the long-running services
-- (generator, spark driver, sar worker, ml trainer) plus Airflow DAG failures,
-- so operators can see what is going wrong from one place (Logs page + SQL
-- Explorer) instead of grepping per-container stdout.

CREATE TABLE IF NOT EXISTS aml.event_log (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    source      VARCHAR(32)  NOT NULL,   -- generator | spark | sar | ml | airflow | dashboard
    level       VARCHAR(16)  NOT NULL,   -- WARNING | ERROR | CRITICAL
    logger      VARCHAR(128),            -- python logger name / dag.task
    message     TEXT         NOT NULL,
    detail      JSONB                    -- traceback, dag context, etc.
);

CREATE INDEX IF NOT EXISTS idx_event_log_ts     ON aml.event_log (ts DESC);
CREATE INDEX IF NOT EXISTS idx_event_log_level  ON aml.event_log (level);
CREATE INDEX IF NOT EXISTS idx_event_log_source ON aml.event_log (source);

-- Expose to the read-only SQL Explorer role (consistent with other aml tables;
-- the schema browser picks it up automatically).
GRANT SELECT ON aml.event_log TO freesql_reader;
