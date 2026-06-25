-- Snapshot of all customer transactions at alert time (investigation evidence chain).
-- Retained 180 days (aligned with flagged_transactions / sar_reports).

CREATE TABLE IF NOT EXISTS aml.alert_transaction_archive (
    id                  BIGSERIAL PRIMARY KEY,
    alert_txn_id        VARCHAR(64) NOT NULL,
    rule_name           VARCHAR(64) NOT NULL,
    flagged_at          TIMESTAMPTZ NOT NULL,
    customer_id         VARCHAR(32) NOT NULL,
    archived_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    txn_id              VARCHAR(64) NOT NULL,
    txn_category        VARCHAR(32),
    txn_type            VARCHAR(32),
    sender_id           VARCHAR(32),
    receiver_id         VARCHAR(64),
    sender_customer_no  VARCHAR(32),
    receiver_customer_no VARCHAR(32),
    sender_name         VARCHAR(255),
    receiver_name       VARCHAR(255),
    sender_identity_no  VARCHAR(32),
    receiver_identity_no VARCHAR(32),
    sender_branch       VARCHAR(16),
    receiver_branch     VARCHAR(16),
    sender_country      VARCHAR(3),
    receiver_country    VARCHAR(3),
    txn_description     VARCHAR(512),
    branch_id           VARCHAR(16),
    amount              NUMERIC(18, 2),
    currency            VARCHAR(3),
    amount_eur          NUMERIC(18, 2),
    country_code        VARCHAR(3),
    is_customer_sender  BOOLEAN,
    is_customer_receiver BOOLEAN,
    is_synthetic_fraud  BOOLEAN,
    fraud_type          VARCHAR(32),
    ts                  TIMESTAMPTZ,
    txn_ingested_at     TIMESTAMPTZ,
    UNIQUE (alert_txn_id, rule_name, txn_id)
);

CREATE INDEX IF NOT EXISTS idx_alert_archive_customer
    ON aml.alert_transaction_archive (customer_id, flagged_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_archive_alert
    ON aml.alert_transaction_archive (alert_txn_id, rule_name);
CREATE INDEX IF NOT EXISTS idx_alert_archive_archived_at
    ON aml.alert_transaction_archive (archived_at);

-- Backfill snapshots for alerts already in the system (best-effort from live transactions).
INSERT INTO aml.alert_transaction_archive (
    alert_txn_id, rule_name, flagged_at, customer_id,
    txn_id, txn_category, txn_type,
    sender_id, receiver_id,
    sender_customer_no, receiver_customer_no,
    sender_name, receiver_name,
    sender_identity_no, receiver_identity_no,
    sender_branch, receiver_branch,
    sender_country, receiver_country, txn_description,
    branch_id, amount, currency, amount_eur, country_code,
    is_customer_sender, is_customer_receiver,
    is_synthetic_fraud, fraud_type, ts, txn_ingested_at
)
SELECT
    f.txn_id,
    f.rule_name,
    f.flagged_at,
    f.customer_id,
    t.txn_id,
    t.txn_category,
    t.txn_type,
    t.sender_id,
    t.receiver_id,
    t.sender_customer_no,
    t.receiver_customer_no,
    t.sender_name,
    t.receiver_name,
    t.sender_identity_no,
    t.receiver_identity_no,
    t.sender_branch,
    t.receiver_branch,
    t.sender_country,
    t.receiver_country,
    t.txn_description,
    t.branch_id,
    t.amount,
    t.currency,
    t.amount_eur,
    t.country_code,
    t.is_customer_sender,
    t.is_customer_receiver,
    t.is_synthetic_fraud,
    t.fraud_type,
    t.ts,
    t.ingested_at
FROM aml.flagged_transactions f
JOIN aml.transactions t
  ON t.sender_customer_no = f.customer_id
  OR t.receiver_customer_no = f.customer_id
ON CONFLICT (alert_txn_id, rule_name, txn_id) DO NOTHING;

GRANT SELECT ON aml.alert_transaction_archive TO freesql_reader;
