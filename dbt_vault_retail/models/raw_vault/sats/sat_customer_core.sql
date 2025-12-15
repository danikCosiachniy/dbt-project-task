{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['h_customer_pk', 'effective_from', 'record_source'],
    tags=['raw_vault', 'sat', 'low_volatility']
) }}

WITH customer_business_date AS (
    SELECT
        customer_id
        , min(order_date)::timestamp AS business_effective_from
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)

, source_data AS (
    SELECT
        c.customer_name
        , c.market_segment
        , c.hd_customer_core AS hashdiff
        , NULL::timestamp AS effective_to
        , {{ record_source('tpch', 'CUSTOMER') }} AS record_source
        , sha2(coalesce(to_varchar(c.customer_id), ''), 256) AS h_customer_pk
        , current_timestamp() AS load_ts
        , coalesce(
            cbd.business_effective_from
            , current_timestamp()
        ) AS effective_from
    FROM {{ ref('stg_customer') }} AS c
    LEFT JOIN customer_business_date AS cbd
        ON c.customer_id = cbd.customer_id
)

{% if is_incremental() %}
    , latest_records AS (
        SELECT
            h_customer_pk
            , record_source
            , hashdiff
        FROM {{ this }}
        QUALIFY row_number() OVER (
            PARTITION BY h_customer_pk, record_source
            ORDER BY effective_from DESC
        ) = 1
    )
{% endif %}

SELECT
    s.customer_name
    , s.market_segment
    , s.effective_to
    , s.record_source
    , s.h_customer_pk
    , s.effective_from
    , s.load_ts
    , s.hashdiff
FROM source_data AS s

{% if is_incremental() %}
    LEFT JOIN latest_records AS l
        ON
            s.h_customer_pk = l.h_customer_pk
            AND s.record_source = l.record_source
    WHERE
        l.h_customer_pk IS NULL
        OR s.hashdiff != l.hashdiff
{% endif %}
