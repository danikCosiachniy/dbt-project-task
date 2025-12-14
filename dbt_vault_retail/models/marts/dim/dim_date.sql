{{ config(materialized='table', tags=['mart', 'dimension']) }}

with bounds as (
    select
        min(pit_date)::date as min_date
        , max(pit_date)::date as max_date
    from {{ ref('pit_order') }}
)

, dates as (
    select dateadd('day', seq4(), b.min_date) as date_day
    from bounds as b
    , table(generator(rowcount => 365 * 50))
    where dateadd('day', seq4(), b.min_date) <= b.max_date
)

select
    date_day as date_key
    , date_day as full_date
    , year(date_day) as year
    , month(date_day) as month
    , day(date_day) as day
    , to_char(date_day, 'YYYY-MM') as year_month
    , to_char(date_day, 'Mon') as month_name
    , dayofweek(date_day) as day_of_week
    , to_char(date_day, 'DY') as day_name
    , coalesce(dayofweek(date_day) in (6, 7), false) as is_weekend
from dates
