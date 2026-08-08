-- Semantic normalization lives here (not in the ingest parser): the staged
-- source arrives with sanitized headers but every column still a string.
{{ config(tags=['orders']) }}

select
    cast(order_id as int) as order_id,
    customer,
    cast(order_date as date) as order_date,
    cast(amount as double) as amount,
    lower(trim(status)) as status
from {{ source('raw', 'orders') }}
