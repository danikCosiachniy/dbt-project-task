{{ config(
    materialized='incremental',
    unique_key='l_order_customer_pk',
    incremental_strategy='merge',
    tags=['raw_vault', 'link']
) }}

WITH src AS (
    SELECT
        o.order_date AS effective_from
        , {{ record_source('tpch', 'ORDERS') }} AS record_source
        , sha2(
            coalesce(to_varchar(o.order_id), '') || '|'
            || coalesce(to_varchar(o.customer_id), '')
            , 256
        ) AS l_order_customer_pk
        , sha2(coalesce(to_varchar(o.order_id), ''), 256) AS h_order_pk
        , sha2(coalesce(to_varchar(o.customer_id), ''), 256) AS h_customer_pk
        , current_timestamp() AS load_ts
    FROM {{ ref('stg_orders') }} AS o
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
