{{ config(tags=['customers']) }}

select
    customer,
    lower(trim(segment)) as segment,
    lower(trim(region)) as region
from {{ source('raw', 'customers') }}
