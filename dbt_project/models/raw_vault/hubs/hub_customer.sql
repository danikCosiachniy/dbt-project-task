{{ config(materialized='incremental') }}

SELECT DISTINCT
    source.order_customer_link_hk
    , source.order_hk
    , source.customer_hk
    , source.record_source
    , CURRENT_TIMESTAMP AS load_date
FROM {{ ref('stg_orders') }} AS source

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS target
        WHERE target.order_customer_link_hk = source.order_customer_link_hk
    )
{% endif %}
