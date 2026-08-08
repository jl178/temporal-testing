{{ config(tags=['payments']) }}

select
    payment_id,
    order_id,
    paid_amount,
    paid_date
from {{ ref('stg_payments') }}
