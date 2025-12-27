{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['raw_vault', 'link']
) }}

WITH src AS (
    SELECT
        record_source
        , l_order_customer_pk
        , h_order_pk
        , h_customer_pk
        , load_ts
    FROM {{ ref('stg_orders') }}
)

SELECT *
FROM src AS s

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE t.l_order_customer_pk = s.l_order_customer_pk
    )
{% endif %}
