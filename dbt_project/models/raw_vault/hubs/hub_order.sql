{{ config(materialized='incremental') }}

SELECT DISTINCT
    source.order_hk
    , source.order_id
    , source.record_source
    , CURRENT_TIMESTAMP AS load_date
FROM {{ ref('stg_orders') }} AS source

{% if is_incremental() %}
-- Incremental logic using NOT EXISTS for better performance
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS target
        WHERE target.order_hk = source.order_hk
    )
{% endif %}
