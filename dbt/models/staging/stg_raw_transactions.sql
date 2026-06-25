select
    txn_id,
    account_id,
    amount,
    currency,
    ts,
    merchant,
    is_synthetic_fraud,
    ingested_at
from {{ source('aml_raw', 'raw_transactions') }}
