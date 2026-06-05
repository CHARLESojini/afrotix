-- One row per issued ticket (a sale). Tickets that were later cancelled are
-- flagged, and net_amount zeroes them out, so gross vs net revenue both fall
-- straight out of this table.

with lifecycle as (
    select * from {{ ref('int_saga_lifecycle') }}
)

select
    saga_id,
    ticket_id,
    event_id,
    tier_id,
    customer_id,

    amount,
    issued_at,
    cast(issued_at as date) as date_day,

    (reason = 'customer_cancellation')                              as is_cancelled,
    case when reason = 'customer_cancellation' then 0 else amount end as net_amount
from lifecycle
where was_issued
