{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT * FROM {{ ref('raw_orders') }}
),

hashed_data AS (
    SELECT
        order_id,
        customer_id,
        SHA2(CAST(order_id AS VARCHAR), 256) as order_hk,
        SHA2(CAST(customer_id AS VARCHAR), 256) as customer_hk,
        SHA2(CAST(order_id AS VARCHAR) || CAST(customer_id AS VARCHAR), 256) as order_customer_link_hk,
        SHA2(CAST(order_date AS VARCHAR) || CAST(status AS VARCHAR), 256) as order_hashdiff,
        order_date,
        status,
        'raw_orders_csv' as record_source,
        CURRENT_TIMESTAMP as load_date
    FROM raw_data
)

SELECT * FROM hashed_data
