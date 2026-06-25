select
    id,
    txn_id,
    account_id,
    amount,
    currency,
    ts,
    merchant,
    rule_name,
    rule_detail,
    flagged_at
from {{ source('aml_raw', 'flagged_transactions') }}
