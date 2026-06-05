with artists as (
    select * from {{ source('bronze', 'raw_artists') }}
)

select
    artist_id,
    name as artist_name,
    subgenre,
    country,
    monthly_listeners
from artists
