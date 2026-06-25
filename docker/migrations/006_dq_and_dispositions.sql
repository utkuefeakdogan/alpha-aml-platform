-- Customer country + alert analyst dispositions
ALTER TABLE aml.customers
    ADD COLUMN IF NOT EXISTS country VARCHAR(3);

UPDATE aml.customers
SET country = CASE (ABS(hashtext(customer_id)) % 10)
    WHEN 0 THEN 'DE' WHEN 1 THEN 'TR' WHEN 2 THEN 'FR' WHEN 3 THEN 'NL'
    WHEN 4 THEN 'AT' WHEN 5 THEN 'BE' WHEN 6 THEN 'IT' WHEN 7 THEN 'ES'
    WHEN 8 THEN 'PL' ELSE 'CH' END
WHERE country IS NULL;

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
