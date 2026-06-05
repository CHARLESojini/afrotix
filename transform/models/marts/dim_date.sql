-- Date dimension built from the dates actually present in the events, so it
-- needs no external data and stays portable across DuckDB and Snowflake
-- (only EXTRACT and a CASE, no dialect-specific date functions).

with dates as (
    select distinct cast(occurred_at as date) as date_day
    from {{ ref('stg_saga_events') }}
    where occurred_at is not null
)

select
    date_day,
    extract(year    from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month   from date_day) as month,
    extract(day     from date_day) as day,
    case extract(month from date_day)
        when 1  then 'January'   when 2  then 'February' when 3  then 'March'
        when 4  then 'April'     when 5  then 'May'      when 6  then 'June'
        when 7  then 'July'      when 8  then 'August'   when 9  then 'September'
        when 10 then 'October'   when 11 then 'November' when 12 then 'December'
    end as month_name
from dates
