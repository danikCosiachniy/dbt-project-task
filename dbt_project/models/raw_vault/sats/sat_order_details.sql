{{ config(materialized='incremental') }}

SELECT
    order_hk,      
    order_hashdiff,
    order_date,
    status,
    load_date,
    record_source
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}

WHERE order_hk || order_hashdiff NOT IN 
    (SELECT order_hk || order_hashdiff FROM {{ this }})
{% endif %}