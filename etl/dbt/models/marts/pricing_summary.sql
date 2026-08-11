{{ config(tags=['settlements']) }}
-- Per-vendor pricing outcomes for a settlement batch: what was asked,
-- what the contracts said, and what the gates decided. `savings` is the
-- headline number — submitted vs actually payable.
select
    vendor,
    count(*)                                               as orders_count,
    sum(case when outcome = 'settled'   then 1 else 0 end) as settled_count,
    sum(case when outcome = 'denied'    then 1 else 0 end) as denied_count,
    sum(case when outcome = 'escalated' then 1 else 0 end) as escalated_count,
    round(sum(submitted_amount), 2)                        as submitted_total,
    round(sum(payable_amount), 2)                          as payable_total,
    round(sum(submitted_amount) - sum(payable_amount), 2)  as savings
from {{ ref('stg_settlements') }}
group by vendor
order by vendor
