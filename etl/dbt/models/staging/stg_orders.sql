select
    cast(order_id as int) as order_id,
    customer,
    cast(order_date as date) as order_date,
    cast(amount as double) as amount,
    lower(status) as status
from {{ source('raw', 'orders') }}
