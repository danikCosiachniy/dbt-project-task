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
        , SHA2(CAST(order_id AS VARCHAR), 256) AS order_hk
        , SHA2(CAST(customer_id AS VARCHAR), 256) AS customer_hk
        , SHA2(CAST(order_id AS VARCHAR) || CAST(customer_id AS VARCHAR), 256) AS order_customer_link_hk
        , SHA2(CAST(order_date AS VARCHAR) || CAST(status AS VARCHAR), 256) AS order_hashdiff
        , CURRENT_TIMESTAMP AS load_date
    FROM raw_data
)

SELECT * FROM hashed_data
