-- Sender/receiver branch & country, transaction description
ALTER TABLE aml.transactions
    ADD COLUMN IF NOT EXISTS sender_branch VARCHAR(16),
    ADD COLUMN IF NOT EXISTS receiver_branch VARCHAR(16),
    ADD COLUMN IF NOT EXISTS sender_country VARCHAR(3),
    ADD COLUMN IF NOT EXISTS receiver_country VARCHAR(3),
    ADD COLUMN IF NOT EXISTS txn_description VARCHAR(512);

UPDATE aml.transactions
SET sender_branch = COALESCE(sender_branch, branch_id),
    sender_country = COALESCE(sender_country, country_code),
    receiver_country = COALESCE(receiver_country, country_code),
    txn_description = COALESCE(txn_description, txn_type || ' — ' || txn_category)
WHERE sender_branch IS NULL OR sender_country IS NULL OR txn_description IS NULL;
