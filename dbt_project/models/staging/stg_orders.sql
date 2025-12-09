{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT * FROM {{ ref('raw_orders') }}
)

, hashed_data AS (
    SELECT
        order_id
        , customer_id
        , order_date
        , status
        , 'raw_orders_csv' AS record_source
        , {{ hash_sha256("order_id") }} AS order_hk
        , {{ hash_sha256("customer_id") }} AS customer_hk
        , {{ hash_sha256("order_id || customer_id") }} AS order_customer_link_hk
        , {{ hash_sha256("order_date || status") }} AS order_hashdiff
        , CURRENT_TIMESTAMP AS load_date
    FROM raw_data
)

SELECT * FROM hashed_data
