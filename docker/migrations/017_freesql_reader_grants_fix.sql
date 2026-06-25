-- Re-apply freesql_reader SELECT grants (tables recreated after 015 lose privileges).

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

REVOKE ALL ON TABLE aml.raw_transactions FROM freesql_reader;
