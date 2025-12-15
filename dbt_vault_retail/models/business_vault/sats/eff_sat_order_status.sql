{{ config(
    materialized='incremental',
    unique_key=['h_order_pk', 'effective_from', 'record_source'],
    incremental_strategy='merge',
    tags=['business_vault', 'sat', 'effectivity']
) }}

WITH src AS (
    SELECT
        order_id
        , order_status
        , order_date::date AS status_change_date
        , {{ record_source('tpch', 'ORDERS') }} AS record_source
        , sha2(coalesce(to_varchar(order_id), ''), 256) AS h_order_pk
    FROM {{ ref('stg_orders') }}
)

, ordered AS (
    SELECT
        h_order_pk
        , order_status
        , status_change_date AS effective_from
        , record_source
        , lead(status_change_date) OVER (
            PARTITION BY h_order_pk
            ORDER BY status_change_date
        ) AS next_change_date
    FROM src
)

, final AS (
    SELECT
        h_order_pk
        , order_status
        , effective_from
        , record_source
        , coalesce(
            next_change_date - INTERVAL '1 day'
            , '9999-12-31'::date
        ) AS effective_to
        , current_timestamp() AS load_ts
    FROM ordered
)

SELECT *
FROM final

{% if is_incremental() %}
    WHERE
        effective_from
        > (SELECT coalesce(max(t.effective_from), '1900-01-01'::date) FROM {{ this }} AS t)
{% endif %}
