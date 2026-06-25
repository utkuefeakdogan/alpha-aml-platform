-- Banking Investigation Ecosystem: Customer 360 + addresses + behavior state
-- Migration 007

ALTER TABLE aml.customers
    ADD COLUMN IF NOT EXISTS onboarding_date DATE,
    ADD COLUMN IF NOT EXISTS onboarding_channel VARCHAR(16),
    ADD COLUMN IF NOT EXISTS customer_status VARCHAR(16) NOT NULL DEFAULT 'active';

UPDATE aml.customers
SET onboarding_date = COALESCE(onboarding_date, (created_at AT TIME ZONE 'UTC')::date),
    onboarding_channel = COALESCE(onboarding_channel,
        (ARRAY['Mobile','Branch','Web','ATM'])[1 + (ABS(hashtext(customer_id)) % 4)])
WHERE onboarding_date IS NULL OR onboarding_channel IS NULL;

-- Dormant cohort: every 5th customer beyond 200
UPDATE aml.customers
SET customer_status = 'dormant'
WHERE customer_id >= 'CUST-00201' AND customer_id <= 'CUST-00250';

-- identity_no uniqueness (resolve dupes first)
UPDATE aml.customers c
SET identity_no = LPAD((ABS(hashtext(c.customer_id)) % 100000000000)::text, 11, '0')
WHERE identity_no IS NULL OR identity_no = '';

CREATE UNIQUE INDEX IF NOT EXISTS uq_customers_identity_no ON aml.customers (identity_no);

CREATE TABLE IF NOT EXISTS aml.customer_addresses (
    address_id      SERIAL PRIMARY KEY,
    customer_id     VARCHAR(32) NOT NULL REFERENCES aml.customers(customer_id),
    city            VARCHAR(128) NOT NULL,
    district        VARCHAR(128),
    country_code    VARCHAR(3) NOT NULL,
    address_type    VARCHAR(32) NOT NULL DEFAULT 'home'
);

CREATE INDEX IF NOT EXISTS idx_customer_addresses_cid ON aml.customer_addresses (customer_id);

CREATE TABLE IF NOT EXISTS aml.customer_behavior_30d (
    customer_id                 VARCHAR(32) PRIMARY KEY REFERENCES aml.customers(customer_id),
    txn_count_30d               INTEGER NOT NULL DEFAULT 0,
    volume_30d                  NUMERIC(18, 2) NOT NULL DEFAULT 0,
    distinct_counterparties_30d INTEGER NOT NULL DEFAULT 0,
    primary_sender_country      VARCHAR(3),
    last_txn_at                 TIMESTAMPTZ,
    computed_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Expand customer universe to 250 (50 dormant) if not present
INSERT INTO aml.customers (
    customer_id, identity_no, name, onboarding_date, onboarding_channel,
    risk_score, segment, is_pep, branch_id, country, customer_status
)
SELECT
    'CUST-' || LPAD(g::text, 5, '0'),
    LPAD((g * 7654321 % 100000000000)::text, 11, '0'),
    'Customer ' || g,
    CURRENT_DATE - (g * 17 % 1825),
    (ARRAY['Mobile','Branch','Web','ATM'])[1 + (g % 4)],
    ROUND((random() * 80 + 5)::numeric, 2),
    CASE WHEN g % 10 = 0 THEN 'corporate' WHEN g % 7 = 0 THEN 'premium' ELSE 'retail' END,
    g % 25 = 0,
    'BR-' || LPAD((1 + (g % 12))::text, 3, '0'),
    (ARRAY['DE','TR','FR','NL','AT','BE','IT','ES','PL','CH'])[1 + (g % 10)],
    CASE WHEN g > 200 THEN 'dormant' ELSE 'active' END
FROM generate_series(1, 250) g
ON CONFLICT (customer_id) DO NOTHING;

-- Seed addresses (home + work for active customers)
INSERT INTO aml.customer_addresses (customer_id, city, district, country_code, address_type)
SELECT
    c.customer_id,
    (ARRAY['Istanbul','Ankara','Izmir','Berlin','Munich','Hamburg','Paris','Amsterdam','Vienna','Brussels'])[1 + (ABS(hashtext(c.customer_id)) % 10)],
    'District ' || (1 + ABS(hashtext(c.customer_id || 'd')) % 20),
    COALESCE(c.country, 'DE'),
    atype
FROM aml.customers c
CROSS JOIN (VALUES ('home'), ('work')) AS t(atype)
WHERE NOT EXISTS (
    SELECT 1 FROM aml.customer_addresses a
    WHERE a.customer_id = c.customer_id AND a.address_type = t.atype
);
