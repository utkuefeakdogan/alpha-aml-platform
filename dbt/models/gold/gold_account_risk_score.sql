select
    account_id,
    count(*) as total_flags,
    count(distinct rule_name) as distinct_rules,
    max(amount) as max_flagged_amount,
    max(flagged_at) as last_flagged_at,
    case
        when count(*) >= 5 then 'high'
        when count(*) >= 2 then 'medium'
        else 'low'
    end as risk_tier
from {{ ref('stg_flagged_transactions') }}
group by account_id
