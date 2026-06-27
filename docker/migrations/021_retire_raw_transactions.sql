-- Retire the legacy aml.raw_transactions table.
--
-- The streaming pipeline writes parsed records directly to aml.transactions
-- (src/processing/streaming_job.py), so raw_transactions was never populated and
-- stayed permanently empty. The true raw landing zone is the Kafka topic
-- transactions.raw (bronze); aml.transactions is the first persisted (silver)
-- layer. Dropping this orphan table removes a misleading "bronze" artifact.
--
-- CASCADE also drops any leftover dbt staging view (stg_raw_transactions) that
-- referenced this table; the dbt model + source entry are removed in the same
-- change so nothing recreates it.

DROP TABLE IF EXISTS aml.raw_transactions CASCADE;
