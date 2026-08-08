-- Cross-route consolidation: joins the tables produced by the orders,
-- customers, and payments transform jobs. Only buildable when a persistent
-- catalog is configured (the upstream tables must exist across jobs).
{{ config(tags=['consolidated']) }}

with completed_orders as (
    select * from {{ ref('stg_orders') }} where status = 'completed'
),

paid as (
    select order_id, sum(paid_amount) as paid_amount
    from {{ ref('fct_payments') }}
    group by order_id
)

select
    c.segment,
    count(distinct o.order_id) as orders_count,
    round(sum(o.amount), 2) as revenue,
    round(coalesce(sum(p.paid_amount), 0), 2) as collected,
    round(coalesce(sum(p.paid_amount), 0) / sum(o.amount), 3) as collection_rate
from completed_orders o
join {{ ref('dim_customers') }} c using (customer)
left join paid p on p.order_id = o.order_id
group by c.segment
order by c.segment
