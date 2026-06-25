-- Phase 2: SAR metadata, customer acquisition log, clean stale SAR data.

ALTER TABLE aml.sar_reports
    ADD COLUMN IF NOT EXISTS filed_by VARCHAR(128);

DELETE FROM aml.sar_reports;

CREATE TABLE IF NOT EXISTS aml.customer_acquisition_log (
    customer_id VARCHAR(32) PRIMARY KEY REFERENCES aml.customers(customer_id),
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_customer_acquisition_day
    ON aml.customer_acquisition_log (acquired_at);
