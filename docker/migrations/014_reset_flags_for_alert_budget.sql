-- Optional: reset inflated flag history after alert-budget fix (run once).
TRUNCATE aml.alert_dispositions, aml.flagged_transactions CASCADE;
