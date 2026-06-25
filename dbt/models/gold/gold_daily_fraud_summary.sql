select
    date_trunc('day', flagged_at)::date as report_date,
    rule_name,
    count(*) as flag_count,
    count(distinct account_id) as distinct_accounts,
    sum(amount) as total_flagged_amount
from {{ ref('stg_flagged_transactions') }}
group by 1, 2
