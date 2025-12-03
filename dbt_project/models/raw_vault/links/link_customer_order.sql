{{ config(materialized='incremental') }}

SELECT DISTINCT
    order_customer_link_hk, 
    order_hk,
    customer_hk,
    load_date,
    record_source
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
WHERE order_customer_link_hk NOT IN (SELECT order_customer_link_hk FROM {{ this }})
{% endif %}