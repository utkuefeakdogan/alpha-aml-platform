-- Migration: enterprise customers + transactions (existing deployments)
CREATE TABLE IF NOT EXISTS aml.customers (
    customer_id     VARCHAR(32) PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    risk_score      NUMERIC(5, 2) NOT NULL DEFAULT 0,
    segment         VARCHAR(32) NOT NULL DEFAULT 'retail',
    is_pep          BOOLEAN NOT NULL DEFAULT FALSE,
    branch_id       VARCHAR(16) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aml.transactions (
    txn_id              VARCHAR(64) PRIMARY KEY,
    txn_category        VARCHAR(32) NOT NULL,
    txn_type            VARCHAR(32) NOT NULL,
    sender_id           VARCHAR(32) NOT NULL,
    receiver_id         VARCHAR(64) NOT NULL,
    branch_id           VARCHAR(16) NOT NULL,
    amount              NUMERIC(18, 2) NOT NULL,
    currency            VARCHAR(3) NOT NULL DEFAULT 'EUR',
    country_code        VARCHAR(3),
    is_customer_sender  BOOLEAN NOT NULL DEFAULT TRUE,
    is_customer_receiver BOOLEAN NOT NULL DEFAULT FALSE,
    is_synthetic_fraud  BOOLEAN DEFAULT FALSE,
    fraud_type          VARCHAR(32),
    ts                  TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aml.account_window_metrics (
    id                      SERIAL PRIMARY KEY,
    customer_id             VARCHAR(32) NOT NULL,
    window_type             VARCHAR(16) NOT NULL,
    total_volume            NUMERIC(18, 2) NOT NULL DEFAULT 0,
    txn_count               INTEGER NOT NULL DEFAULT 0,
    distinct_receiver_count INTEGER NOT NULL DEFAULT 0,
    window_start            TIMESTAMPTZ NOT NULL,
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (customer_id, window_type, window_start)
);

ALTER TABLE aml.flagged_transactions
    ADD COLUMN IF NOT EXISTS customer_id VARCHAR(32),
    ADD COLUMN IF NOT EXISTS window_type VARCHAR(16);

ALTER TABLE aml.sar_reports
    ADD COLUMN IF NOT EXISTS customer_id VARCHAR(32);

INSERT INTO aml.customers (customer_id, name, risk_score, segment, is_pep, branch_id)
SELECT
    'CUST-' || LPAD(g::text, 5, '0'),
    'Customer ' || g,
    ROUND((random() * 80 + 5)::numeric, 2),
    CASE WHEN g % 10 = 0 THEN 'corporate' WHEN g % 7 = 0 THEN 'premium' ELSE 'retail' END,
    g % 25 = 0,
    'BR-' || LPAD((1 + (g % 12))::text, 3, '0')
FROM generate_series(1, 200) g
ON CONFLICT (customer_id) DO NOTHING;
