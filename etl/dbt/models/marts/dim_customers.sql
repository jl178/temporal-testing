{{ config(tags=['customers']) }}

select
    customer,
    segment,
    region
from {{ ref('stg_customers') }}
