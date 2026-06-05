-- Staging for saga events: type the columns and lift the per-event fields out
-- of the JSON/VARIANT payload into proper columns. One row per emitted event.

with source as (
    select * from {{ source('bronze', 'raw_saga_events') }}
)

select
    saga_id,
    event_type,
    step,
    customer_id,
    event_id,
    tier_id,
    amount,
    occurred_at,

    -- fields that live inside the payload (cross-adapter extraction)
    {{ extract_json('payload', 'status') }}                        as status,
    {{ extract_json('payload', 'reason') }}                        as reason,
    cast({{ extract_json('payload', 'failed_step') }} as integer)  as failed_step,
    cast({{ extract_json('payload', 'latency_ms') }} as double)    as latency_ms,
    {{ extract_json('payload', 'ticket_id') }}                     as ticket_id,
    {{ extract_json('payload', 'hold_id') }}                       as hold_id,
    {{ extract_json('payload', 'payment_id') }}                    as payment_id,

    _row_hash,
    _loaded_at
from source
