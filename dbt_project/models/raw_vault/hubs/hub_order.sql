{{ config(materialized='incremental') }}

SELECT DISTINCT
    source.order_hk,
    source.order_id,
    CURRENT_TIMESTAMP as load_date,
    source.record_source
FROM {{ ref('stg_orders') }} as source

{% if is_incremental() %}
-- Incremental logic using NOT EXISTS for better performance
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} as target
    WHERE target.order_hk = source.order_hk
)
{% endif %}
