{{ config(
    materialized='incremental',
    unique_key='h_order_pk',
    incremental_strategy='merge',
    tags=['raw_vault', 'hub']
) }}

WITH src AS (
    SELECT DISTINCT
        order_id AS bk_order_id
        , {{ record_source('tpch', 'ORDERS') }} AS record_source
        , sha2(coalesce(to_varchar(order_id), ''), 256) AS h_order_pk
        , current_timestamp() AS load_ts
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
