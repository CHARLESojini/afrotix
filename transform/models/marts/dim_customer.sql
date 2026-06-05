with customers as (
    select * from {{ source('bronze', 'raw_customers') }}
)

select
    customer_id,
    name as customer_name,
    email,
    city,
    loyalty_tier,
    created_at
from customers
