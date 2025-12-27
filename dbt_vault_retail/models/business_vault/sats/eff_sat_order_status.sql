{{ config(
    materialized='incremental',
    unique_key=['h_order_pk', 'effective_from', 'record_source'],
    incremental_strategy='merge',
    tags=['business_vault', 'sat', 'effectivity']
) }}

WITH link AS (
    SELECT
        h_order_pk
        , record_source
    FROM {{ ref('lnk_order_customer') }}
)

, sat AS (
    SELECT
        h_order_pk
        , record_source
        , order_status
        , load_ts AS status_change_ts
    FROM {{ ref('sat_order_core') }}
)

, src AS (
    SELECT
        l.h_order_pk
        , l.record_source
        , s.order_status
        , s.status_change_ts
    FROM link AS l
    INNER JOIN sat AS s
        ON
            l.h_order_pk = s.h_order_pk
            AND l.record_source = s.record_source
)

, ordered AS (
    SELECT
        h_order_pk
        , record_source
        , order_status
        , status_change_ts AS effective_from
        , lead(status_change_ts) OVER (
            PARTITION BY h_order_pk, record_source
            ORDER BY status_change_ts
        ) AS next_change_ts
    FROM src
)

, final AS (
    SELECT
        h_order_pk
        , record_source
        , order_status
        , effective_from
        , effective_from AS load_ts
        , coalesce(
            next_change_ts
            , to_timestamp_tz('9999-12-31')
        ) AS effective_to
    FROM ordered
)

SELECT * FROM final
