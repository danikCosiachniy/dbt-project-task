{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['raw_vault', 'hub']
) }}

WITH src AS (
    SELECT
        order_id AS bk_order_id
        , record_source
        , h_order_pk
        , load_ts
    FROM {{ ref('stg_orders') }}
)

SELECT *
FROM src AS s

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE t.h_order_pk = s.h_order_pk
    )
{% endif %}
