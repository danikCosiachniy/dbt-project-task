{{ config(materialized='incremental') }}

SELECT DISTINCT
    source.customer_hk
    , source.customer_id
    , source.record_source
    , CURRENT_TIMESTAMP AS load_date
FROM {{ ref('stg_orders') }} AS source

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS target
        WHERE target.customer_hk = source.customer_hk
    )
{% endif %}
