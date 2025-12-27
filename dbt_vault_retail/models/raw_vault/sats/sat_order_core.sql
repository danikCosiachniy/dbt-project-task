{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['raw_vault', 'sat']
) }}

WITH src AS (
    SELECT
        h_order_pk
        , order_status
        , total_price
        , order_date
        , order_priority
        , clerk
        , ship_priority
        , order_comment
        , record_source
        , load_ts
        , hashdiff
    FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}

    where order_date >= dateadd(
        day, 0,
        (select coalesce(max(order_date), to_date('1992-01-01')) from {{ this }})

    )
    {% endif %}
)

{% if is_incremental() %}
, latest AS (
    SELECT
        h_order_pk
        , record_source
        , hashdiff
    FROM {{ this }}
    QUALIFY row_number() OVER (
        PARTITION BY h_order_pk, record_source
        ORDER BY load_ts DESC
    ) = 1
)
{% endif %}

SELECT
    s.h_order_pk
    , s.order_status
    , s.total_price
    , s.order_date
    , s.order_priority
    , s.clerk
    , s.ship_priority
    , s.order_comment
    , s.record_source
    , s.load_ts
    , s.hashdiff
FROM src AS s

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest AS l
        WHERE s.h_order_pk = l.h_order_pk
          AND s.record_source = l.record_source
          AND s.hashdiff = l.hashdiff
    )
{% endif %}
