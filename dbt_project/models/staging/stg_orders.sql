WITH raw_data AS (
    SELECT * FROM {{ ref('raw_orders') }}
),

hashed_data AS (
    SELECT
        order_id,
        customer_id,
        MD5(CAST(order_id AS VARCHAR)) as order_hk,
        MD5(CAST(customer_id AS VARCHAR)) as customer_hk,
        MD5(CAST(order_id AS VARCHAR) || CAST(customer_id AS VARCHAR)) as order_customer_link_hk,
        MD5(CAST(order_date AS VARCHAR) || CAST(status AS VARCHAR)) as order_hashdiff,
        order_date,
        status,
        'raw_orders_csv' as record_source,
        CURRENT_TIMESTAMP as load_date
    FROM raw_data
)

SELECT * FROM hashed_data