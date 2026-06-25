-- Dedicated read-only role for the dashboard SQL Explorer page.
-- Grants SELECT on investigation/compliance tables only (no raw landing zone).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'freesql_reader') THEN
        CREATE ROLE freesql_reader LOGIN PASSWORD 'freesql_readonly_2026';
    END IF;
END $$;

ALTER ROLE freesql_reader WITH PASSWORD 'freesql_readonly_2026';
ALTER ROLE freesql_reader SET default_transaction_read_only = on;
ALTER ROLE freesql_reader SET statement_timeout = '8s';

REVOKE ALL ON SCHEMA public FROM freesql_reader;
GRANT CONNECT ON DATABASE datadb TO freesql_reader;
GRANT USAGE ON SCHEMA aml TO freesql_reader;

GRANT SELECT ON TABLE
    aml.customers,
    aml.customer_addresses,
    aml.customer_behavior_30d,
    aml.customer_acquisition_log,
    aml.transactions,
    aml.flagged_transactions,
    aml.alert_dispositions,
    aml.sar_reports,
    aml.account_window_metrics,
    aml.pipeline_metrics,
    aml.alert_transaction_archive
TO freesql_reader;

-- Explicit deny on operational / sensitive landing tables.
REVOKE ALL ON TABLE aml.raw_transactions FROM freesql_reader;
