{{ config(tags=['payments']) }}

select
    cast(payment_id as int) as payment_id,
    cast(order_id as int) as order_id,
    cast(paid_amount as double) as paid_amount,
    cast(paid_date as date) as paid_date
from {{ source('raw', 'payments') }}
