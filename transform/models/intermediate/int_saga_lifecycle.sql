-- Collapse the event stream into ONE row per saga. This is the pivot that makes
-- saga-level analytics possible: timestamps per step, the terminal outcome from
-- the CLOSED event, and flags for what actually happened.

with events as (
    select * from {{ ref('stg_saga_events') }}
)

select
    saga_id,
    max(customer_id) as customer_id,
    max(event_id)    as event_id,
    max(tier_id)     as tier_id,
    max(amount)      as amount,

    -- per-step timestamps
    max(case when event_type = 'reserved' then occurred_at end) as reserved_at,
    max(case when event_type = 'charged'  then occurred_at end) as charged_at,
    max(case when event_type = 'issued'   then occurred_at end) as issued_at,
    max(case when event_type = 'closed'   then occurred_at end) as closed_at,

    -- the issued ticket (if any)
    max(case when event_type = 'issued' then ticket_id end) as ticket_id,

    -- terminal outcome, carried on the CLOSED event
    max(case when event_type = 'closed' then status end)      as final_status,
    max(case when event_type = 'closed' then reason end)      as reason,
    max(case when event_type = 'closed' then failed_step end) as failed_step,
    max(case when event_type = 'closed' then latency_ms end)  as latency_ms,

    -- what happened along the way
    sum(case when event_type = 'issued' then 1 else 0 end) > 0 as was_issued,
    sum(case when event_type in ('released', 'refunded', 'voided')
             then 1 else 0 end) > 0                            as was_compensated
from events
group by saga_id
