select
    order_date,
    count(*) as order_count,
    sum(amount) as total_revenue
from {{ ref('stg_orders') }}
where status = 'completed'
group by order_date
order by order_date
