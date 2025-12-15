{{ config(
    materialized='incremental',
    unique_key=['h_order_pk', 'h_customer_pk', 'pit_date'],
    incremental_strategy='merge',
    tags=['business_vault', 'pit']
) }}

WITH spine AS (
    SELECT DISTINCT
        h_order_pk
        , h_customer_pk
        , effective_from::date AS pit_date
    FROM {{ ref('lnk_order_customer') }}
)

SELECT
    s.h_order_pk
    , s.h_customer_pk
    , s.pit_date
    , {{ record_source('tpch', 'ORDERS') }} AS record_source
    , current_timestamp() AS load_ts
FROM spine AS s

{% if is_incremental() %}
    WHERE
        s.pit_date > (
            SELECT coalesce(max(t.pit_date), '1900-01-01'::date)
            FROM {{ this }} AS t
        )
{% endif %}
