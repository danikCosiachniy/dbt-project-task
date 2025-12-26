{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['raw_vault', 'link']
) }}

WITH src AS (
    SELECT
        l_order_lineitem_pk
        , h_order_pk
        , h_product_pk
        , record_source
        , load_ts
    FROM {{ ref('stg_lineitem') }}
)

SELECT *
FROM src
{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE t.l_order_lineitem_pk = src.l_order_lineitem_pk
    )
{% endif %}
