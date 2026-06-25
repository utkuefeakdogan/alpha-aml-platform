-- Scale customer universe to 1,233,000 customers (addresses: migration 013, 1–5 each).
-- Run after operational reset (008) or on fresh deploy. May take several minutes.

TRUNCATE aml.alert_dispositions, aml.flagged_transactions, aml.sar_reports,
         aml.account_window_metrics, aml.customer_behavior_30d,
         aml.transactions, aml.raw_transactions,
         aml.customer_acquisition_log, aml.customer_addresses,
         aml.customers CASCADE;

INSERT INTO aml.customers (
    customer_id, identity_no, name, onboarding_date, onboarding_channel,
    risk_score, segment, is_pep, branch_id, country, customer_status
)
SELECT
    'CUST-' || LPAD(g::text, 7, '0'),
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
    CASE WHEN g % 20 = 0 THEN 'dormant' ELSE 'active' END
FROM generate_series(1, 1233000) AS g;

ANALYZE aml.customers;
