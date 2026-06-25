-- Reset operational AML data; preserve KYC master (customers + addresses).
TRUNCATE aml.alert_dispositions, aml.flagged_transactions, aml.sar_reports,
         aml.account_window_metrics, aml.customer_behavior_30d,
         aml.transactions, aml.raw_transactions CASCADE;
