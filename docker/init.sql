-- Enterprise Banking Transaction Intelligence schema (v2)
CREATE SCHEMA IF NOT EXISTS aml;

-- Legacy raw table (retained for lineage / migration)
CREATE TABLE IF NOT EXISTS aml.raw_transactions (
    txn_id          VARCHAR(64) PRIMARY KEY,
    account_id      VARCHAR(64) NOT NULL,
    amount          NUMERIC(18, 2) NOT NULL,
    currency        VARCHAR(3) NOT NULL DEFAULT 'EUR',
    ts              TIMESTAMPTZ NOT NULL,
    merchant        VARCHAR(255),
    country_code    VARCHAR(3),
    is_synthetic_fraud BOOLEAN DEFAULT FALSE,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Customers (Customer 360 anchor)
CREATE TABLE IF NOT EXISTS aml.customers (
    customer_id         VARCHAR(32) PRIMARY KEY,
    identity_no         VARCHAR(32) UNIQUE NOT NULL,
    name                VARCHAR(255) NOT NULL,
    onboarding_date     DATE NOT NULL,
    onboarding_channel  VARCHAR(16) NOT NULL,
    risk_score          NUMERIC(5, 2) NOT NULL DEFAULT 0,
    segment             VARCHAR(32) NOT NULL DEFAULT 'retail',
    is_pep              BOOLEAN NOT NULL DEFAULT FALSE,
    branch_id           VARCHAR(16) NOT NULL,
    customer_status     VARCHAR(16) NOT NULL DEFAULT 'active',
    country             VARCHAR(3) NOT NULL DEFAULT 'DE',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_customers_status ON aml.customers (customer_status);

-- Customer addresses (KYC / investigation context)
CREATE TABLE IF NOT EXISTS aml.customer_addresses (
    address_id      SERIAL PRIMARY KEY,
    customer_id     VARCHAR(32) NOT NULL REFERENCES aml.customers(customer_id),
    city            VARCHAR(128) NOT NULL,
    district        VARCHAR(128),
    country_code    VARCHAR(3) NOT NULL,
    address_type    VARCHAR(32) NOT NULL DEFAULT 'home'
);

CREATE INDEX IF NOT EXISTS idx_customer_addresses_cid ON aml.customer_addresses (customer_id);

-- 30-day behavior profile (maintained by Spark)
CREATE TABLE IF NOT EXISTS aml.customer_behavior_30d (
    customer_id                 VARCHAR(32) PRIMARY KEY REFERENCES aml.customers(customer_id),
    txn_count_30d               INTEGER NOT NULL DEFAULT 0,
    volume_30d                  NUMERIC(18, 2) NOT NULL DEFAULT 0,
    distinct_counterparties_30d INTEGER NOT NULL DEFAULT 0,
    primary_sender_country      VARCHAR(3),
    last_txn_at                 TIMESTAMPTZ,
    computed_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Enterprise transactions
CREATE TABLE IF NOT EXISTS aml.transactions (
    txn_id                  VARCHAR(64) PRIMARY KEY,
    txn_category            VARCHAR(32) NOT NULL,
    txn_type                VARCHAR(32) NOT NULL,
    sender_id               VARCHAR(32),
    receiver_id             VARCHAR(64),
    sender_customer_no      VARCHAR(32),
    receiver_customer_no    VARCHAR(32),
    sender_name             VARCHAR(255),
    receiver_name           VARCHAR(255),
    sender_identity_no      VARCHAR(32),
    receiver_identity_no    VARCHAR(32),
    sender_branch           VARCHAR(16),
    receiver_branch         VARCHAR(16),
    sender_country          VARCHAR(3),
    receiver_country        VARCHAR(3),
    txn_description         VARCHAR(512),
    branch_id               VARCHAR(16) NOT NULL,
    amount                  NUMERIC(18, 2) NOT NULL,
    currency                VARCHAR(3) NOT NULL DEFAULT 'EUR',
    amount_eur              NUMERIC(18, 2) NOT NULL,
    country_code            VARCHAR(3),
    is_customer_sender      BOOLEAN NOT NULL DEFAULT TRUE,
    is_customer_receiver    BOOLEAN NOT NULL DEFAULT FALSE,
    ts                      TIMESTAMPTZ NOT NULL,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_txn_sender_ts ON aml.transactions (sender_id, ts);
CREATE INDEX IF NOT EXISTS idx_txn_receiver ON aml.transactions (receiver_id);
CREATE INDEX IF NOT EXISTS idx_txn_category ON aml.transactions (txn_category);
CREATE INDEX IF NOT EXISTS idx_txn_ingested ON aml.transactions (ingested_at);

-- Multi-window aggregated metrics (Silver layer — operational)
CREATE TABLE IF NOT EXISTS aml.account_window_metrics (
    id                      SERIAL PRIMARY KEY,
    customer_id             VARCHAR(32) NOT NULL REFERENCES aml.customers(customer_id),
    window_type             VARCHAR(16) NOT NULL,
    total_volume            NUMERIC(18, 2) NOT NULL DEFAULT 0,
    txn_count               INTEGER NOT NULL DEFAULT 0,
    distinct_receiver_count INTEGER NOT NULL DEFAULT 0,
    window_start            TIMESTAMPTZ NOT NULL,
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (customer_id, window_type, window_start)
);

CREATE INDEX IF NOT EXISTS idx_metrics_customer ON aml.account_window_metrics (customer_id, window_type);

-- Flagged alerts
CREATE TABLE IF NOT EXISTS aml.flagged_transactions (
    id              SERIAL PRIMARY KEY,
    txn_id          VARCHAR(64) NOT NULL,
    customer_id     VARCHAR(32),
    account_id      VARCHAR(64),
    amount          NUMERIC(18, 2) NOT NULL,
    amount_eur      NUMERIC(18, 2),
    currency        VARCHAR(3) NOT NULL DEFAULT 'EUR',
    ts              TIMESTAMPTZ NOT NULL,
    merchant        VARCHAR(255),
    rule_name       VARCHAR(64) NOT NULL,
    rule_detail     TEXT,
    window_type     VARCHAR(16),
    flagged_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_priority_score NUMERIC(5, 2),
    UNIQUE (txn_id, rule_name)
);

CREATE INDEX IF NOT EXISTS idx_flagged_customer ON aml.flagged_transactions (customer_id);
CREATE INDEX IF NOT EXISTS idx_flagged_at ON aml.flagged_transactions (flagged_at);

-- Analyst alert dispositions (false positive, SAR filed, etc.)
CREATE TABLE IF NOT EXISTS aml.alert_dispositions (
    id              SERIAL PRIMARY KEY,
    txn_id          VARCHAR(64) NOT NULL,
    rule_name       VARCHAR(64) NOT NULL,
    disposition     VARCHAR(32) NOT NULL,
    analyst_notes   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (txn_id, rule_name)
);

CREATE INDEX IF NOT EXISTS idx_disposition_created ON aml.alert_dispositions (created_at);

-- SAR reports
CREATE TABLE IF NOT EXISTS aml.sar_reports (
    id              SERIAL PRIMARY KEY,
    report_id       VARCHAR(64) UNIQUE NOT NULL,
    account_id_hash VARCHAR(64) NOT NULL,
    customer_id     VARCHAR(32),
    flagged_count   INTEGER NOT NULL,
    report_text     TEXT NOT NULL,
    model_used      VARCHAR(64),
    filed_by        VARCHAR(128),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed customer universe (dev bootstrap: 250 rows).
-- Production scale: apply docker/migrations/011_customer_universe_1_23m.sql (1,233,000 + 2 addresses each).
INSERT INTO aml.customers (
    customer_id, identity_no, name, onboarding_date, onboarding_channel,
    risk_score, segment, is_pep, branch_id, country, customer_status
)
SELECT
    'CUST-' || LPAD(g::text, 5, '0'),
    LPAD((g::bigint * 7654321 % 100000000000)::text, 11, '0'),
    'Customer ' || g,
    CURRENT_DATE - (g * 17 % 1825),
    (ARRAY['Mobile','Branch','Web','ATM'])[1 + (g % 4)],
    LEAST(
        100,
        CASE
            WHEN g % 10 = 0 THEN 45
            WHEN g % 7 = 0 THEN 30
            ELSE 15
        END
        + CASE WHEN g % 25 = 0 THEN 25 ELSE 0 END
        + CASE (ARRAY['Mobile','Branch','Web','ATM'])[1 + (g % 4)]
            WHEN 'Branch' THEN 0
            WHEN 'Mobile' THEN 5
            WHEN 'Web' THEN 10
            WHEN 'ATM' THEN 15
            ELSE 0
        END
        + CASE
            WHEN (ARRAY['DE','TR','FR','NL','AT','BE','IT','ES','PL','CH'])[1 + (g % 10)]
                IN ('TR', 'RU', 'KP', 'IR', 'SY', 'CU') THEN 10
            ELSE 0
        END
    )::numeric,
    CASE WHEN g % 10 = 0 THEN 'corporate' WHEN g % 7 = 0 THEN 'premium' ELSE 'retail' END,
    g % 25 = 0,
    'BR-' || LPAD((1 + (g % 12))::text, 3, '0'),
    (ARRAY['DE','TR','FR','NL','AT','BE','IT','ES','PL','CH'])[1 + (g % 10)],
    CASE WHEN g > 200 THEN 'dormant' ELSE 'active' END
FROM generate_series(1, 250) g
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO aml.customer_addresses (customer_id, city, district, country_code, address_type)
SELECT
    c.customer_id,
    (ARRAY['Istanbul','Ankara','Izmir','Berlin','Munich','Hamburg','Paris','Amsterdam','Vienna','Brussels'])[
        1 + (ABS(hashtext(c.customer_id || ':city')) % 10)
    ],
    'District ' || (1 + ABS(hashtext(c.customer_id || ':d' || s.n::text)) % 20),
    COALESCE(c.country, 'DE'),
    (ARRAY['home','work','mailing','billing','branch'])[
        1 + ((ABS(hashtext(c.customer_id || ':t')) + s.n) % 5)
    ]
FROM aml.customers c
CROSS JOIN LATERAL generate_series(
    1,
    1 + (ABS(hashtext(c.customer_id || ':n')) % 5)
) AS s(n);

CREATE TABLE IF NOT EXISTS aml.customer_acquisition_log (
    customer_id     VARCHAR(32) PRIMARY KEY REFERENCES aml.customers(customer_id),
    acquired_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
