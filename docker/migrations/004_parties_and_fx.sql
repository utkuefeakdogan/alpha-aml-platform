-- Party identification, multi-currency, customer identity
ALTER TABLE aml.customers
    ADD COLUMN IF NOT EXISTS identity_no VARCHAR(32);

UPDATE aml.customers
SET identity_no = LPAD((ABS(hashtext(customer_id)) % 100000000000)::text, 11, '0')
WHERE identity_no IS NULL;

ALTER TABLE aml.transactions
    ADD COLUMN IF NOT EXISTS amount_eur NUMERIC(18, 2),
    ADD COLUMN IF NOT EXISTS sender_customer_no VARCHAR(32),
    ADD COLUMN IF NOT EXISTS receiver_customer_no VARCHAR(32),
    ADD COLUMN IF NOT EXISTS sender_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS receiver_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS sender_identity_no VARCHAR(32),
    ADD COLUMN IF NOT EXISTS receiver_identity_no VARCHAR(32);

-- Allow external inbound (no internal sender customer number)
ALTER TABLE aml.transactions ALTER COLUMN sender_id DROP NOT NULL;

UPDATE aml.transactions
SET amount_eur = amount,
    sender_customer_no = sender_id,
    sender_name = 'Customer ' || sender_id
WHERE amount_eur IS NULL;

ALTER TABLE aml.flagged_transactions
    ADD COLUMN IF NOT EXISTS amount_eur NUMERIC(18, 2);

UPDATE aml.flagged_transactions SET amount_eur = amount WHERE amount_eur IS NULL;
