{{ config(
    materialized='incremental',
    unique_key=['h_order_pk', 'pit_date'],
    incremental_strategy='merge',
    tags=['business_vault', 'pit']
) }}

WITH base AS (
    SELECT DISTINCT
        h_order_pk
        , effective_from::date AS pit_date
    FROM {{ ref('lnk_order_customer') }}
)

, eff_sat AS (
    SELECT
        h_order_pk
        , order_status
        , effective_from
        , effective_to
    FROM {{ ref('eff_sat_order_status') }}
)

SELECT
    b.h_order_pk
    , b.pit_date
    , s.order_status
    , {{ record_source('tpch', 'ORDERS') }} AS record_source
    , current_timestamp() AS load_ts
FROM base AS b
LEFT JOIN eff_sat AS s
    ON
        b.h_order_pk = s.h_order_pk
        AND b.pit_date BETWEEN s.effective_from AND s.effective_to

{% if is_incremental() %}
    WHERE
        b.pit_date > (
            SELECT coalesce(max(t.pit_date), '1900-01-01'::date)
            FROM {{ this }} AS t
        )
{% endif %}
