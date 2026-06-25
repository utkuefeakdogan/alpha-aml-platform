select
    t.sender_id as customer_id,
    c.name,
    c.segment,
    c.risk_score,
    c.is_pep,
    count(*) as txn_count_30d,
    sum(t.amount) as volume_30d,
    count(distinct t.receiver_id) as distinct_receivers_30d,
    max(t.amount) as max_txn_30d,
    count(f.txn_id) as flag_count_30d
from {{ source('aml_enterprise', 'transactions') }} t
join {{ source('aml_enterprise', 'customers') }} c on c.customer_id = t.sender_id
left join {{ source('aml_raw', 'flagged_transactions') }} f
    on f.customer_id = t.sender_id and f.flagged_at >= now() - interval '30 days'
where t.ts >= now() - interval '30 days'
group by t.sender_id, c.name, c.segment, c.risk_score, c.is_pep
