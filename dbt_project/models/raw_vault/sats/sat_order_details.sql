{{ config(materialized='incremental') }}

SELECT
    source.order_hk,
    source.order_hashdiff,
    source.order_date,
    source.status,
    CURRENT_TIMESTAMP as load_date,
    source.record_source
FROM {{ ref('stg_orders') }} as source

{% if is_incremental() %}
-- Insert only if the Hash Diff changed for this Hub Key
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} as target
    WHERE target.order_hk = source.order_hk
      AND target.order_hashdiff = source.order_hashdiff
)
{% endif %}
