-- THE centerpiece mart. One row per purchase attempt, framed for analysis:
-- did it complete or compensate, why, at which step, how long it took, and what
-- it was worth. This is what turns the saga into data-engineering insight.

with lifecycle as (
    select * from {{ ref('int_saga_lifecycle') }}
)

select
    saga_id,
    customer_id,
    event_id,
    tier_id,

    final_status as status,
    reason,
    failed_step,

    amount,
    latency_ms,
    closed_at,
    cast(closed_at as date) as date_day,

    (final_status = 'completed')   as is_completed,
    (final_status = 'compensated') as is_compensated
from lifecycle
