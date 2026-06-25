ALTER TABLE aml.raw_transactions
    ADD COLUMN IF NOT EXISTS country_code VARCHAR(3);
