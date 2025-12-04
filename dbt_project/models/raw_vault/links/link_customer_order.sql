{{ config(materialized='incremental') }}

SELECT DISTINCT
    source.order_customer_link_hk,
    source.order_hk,
    source.customer_hk,
    CURRENT_TIMESTAMP as load_date,
    source.record_source
FROM {{ ref('stg_orders') }} as source

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} as target
    WHERE target.order_customer_link_hk = source.order_customer_link_hk
)
{% endif %}
