{{ config(materialized='table', tags=['mart', 'dimension']) }}

WITH bounds AS (
    SELECT
        min(pit_date)::date AS min_date
        , max(pit_date)::date AS max_date
    FROM {{ ref('pit_order') }}
)

, dates AS (
    SELECT dateadd('day', seq4(), b.min_date) AS date_day
    FROM bounds AS b
    , table(generator(rowcount => 365 * 50))
    WHERE dateadd('day', seq4(), b.min_date) <= b.max_date
)

SELECT
    date_day AS date_key
    , date_day AS full_date
    , year(date_day) AS year
    , month(date_day) AS month
    , day(date_day) AS day
    , to_char(date_day, 'YYYY-MM') AS year_month
    , to_char(date_day, 'Mon') AS month_name
    , dayofweek(date_day) AS day_of_week
    , to_char(date_day, 'DY') AS day_name
    , coalesce(dayofweek(date_day) IN (6, 7), FALSE) AS is_weekend
FROM dates
