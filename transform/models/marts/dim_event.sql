-- Event dimension, denormalized with its artist and venue so a single join
-- enriches any fact with the full who/where/when of a show.

with events as (
    select * from {{ source('bronze', 'raw_events') }}
),
artists as (
    select * from {{ source('bronze', 'raw_artists') }}
),
venues as (
    select * from {{ source('bronze', 'raw_venues') }}
)

select
    e.event_id,
    e.name        as event_name,
    e.event_date,
    e.status      as event_status,

    a.artist_id,
    a.name        as artist_name,
    a.subgenre,

    v.venue_id,
    v.name        as venue_name,
    v.city        as venue_city,
    v.country     as venue_country,
    v.capacity    as venue_capacity
from events e
left join artists a on e.artist_id = a.artist_id
left join venues  v on e.venue_id  = v.venue_id
