-- Alert priority score + deterministic KYC risk_score backfill.

ALTER TABLE aml.flagged_transactions
    ADD COLUMN IF NOT EXISTS alert_priority_score NUMERIC(5, 2);

-- Deterministic KYC risk_score for existing customers
UPDATE aml.customers c
SET risk_score = LEAST(
    100,
    CASE c.segment
        WHEN 'corporate' THEN 45
        WHEN 'premium' THEN 30
        ELSE 15
    END
    + CASE WHEN c.is_pep THEN 25 ELSE 0 END
    + CASE c.onboarding_channel
        WHEN 'Branch' THEN 0
        WHEN 'Mobile' THEN 5
        WHEN 'Web' THEN 10
        WHEN 'ATM' THEN 15
        ELSE 0
    END
    + CASE WHEN c.country IN ('TR', 'RU', 'KP', 'IR', 'SY', 'CU') THEN 10 ELSE 0 END
);

-- Backfill alert_priority_score for existing flags
UPDATE aml.flagged_transactions f
SET alert_priority_score = ROUND(
    (
        0.60 * LEAST(
            100,
            CASE f.rule_name
                WHEN 'geographic' THEN 90
                WHEN 'high_value' THEN 85
                WHEN 'smurfing' THEN 80
                WHEN 'weekly_volume' THEN 70
                WHEN 'daily_velocity' THEN 65
                WHEN 'monthly_peer_anomaly' THEN 60
                ELSE 50
            END
            + LEAST(COALESCE(f.amount_eur, f.amount, 0) / 500.0, 10)
        )
        + 0.40 * COALESCE(c.risk_score, 0)
    )::numeric,
    2
)
FROM aml.customers c
WHERE c.customer_id = f.customer_id;

UPDATE aml.flagged_transactions f
SET alert_priority_score = ROUND(
  0.60 * LEAST(
      100,
      CASE f.rule_name
          WHEN 'geographic' THEN 90
          WHEN 'high_value' THEN 85
          WHEN 'smurfing' THEN 80
          WHEN 'weekly_volume' THEN 70
          WHEN 'daily_velocity' THEN 65
          WHEN 'monthly_peer_anomaly' THEN 60
          ELSE 50
      END
      + LEAST(COALESCE(f.amount_eur, f.amount, 0) / 500.0, 10)
  )::numeric,
  2
)
WHERE f.alert_priority_score IS NULL;

CREATE INDEX IF NOT EXISTS idx_flagged_priority ON aml.flagged_transactions (alert_priority_score DESC);
