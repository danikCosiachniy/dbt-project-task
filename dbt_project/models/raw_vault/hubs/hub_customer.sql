{{ config(materialized='incremental') }}

SELECT DISTINCT
    customer_hk,
    customer_id,
    load_date,
    record_source
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
WHERE customer_hk NOT IN (SELECT customer_hk FROM {{ this }})
{% endif %}
