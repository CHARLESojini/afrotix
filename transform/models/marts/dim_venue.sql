with venues as (
    select * from {{ source('bronze', 'raw_venues') }}
)

select
    venue_id,
    name as venue_name,
    city,
    country,
    capacity
from venues
