{{ config(materialized='incremental') }}

SELECT
    source.order_hk
    , source.order_hashdiff
    , source.order_date
    , source.status
    , source.record_source
    , CURRENT_TIMESTAMP AS load_date
FROM {{ ref('stg_orders') }} AS source

{% if is_incremental() %}
-- Insert only if the Hash Diff changed for this Hub Key
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS target
        WHERE
            target.order_hk = source.order_hk
            AND target.order_hashdiff = source.order_hashdiff
    )
{% endif %}
