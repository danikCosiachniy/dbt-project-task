{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['raw_vault', 'sat', 'low_volatility']
) }}

WITH source_data AS (
    SELECT
        h_customer_pk
        , customer_name
        , market_segment
        , hd_customer_core AS hashdiff
        , record_source
        , load_ts
    FROM {{ ref('stg_customer') }}
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
            ORDER BY load_ts DESC
        ) = 1
    )
{% endif %}

SELECT
    s.customer_name
    , s.market_segment
    , s.record_source
    , s.h_customer_pk
    , s.load_ts
    , s.hashdiff
FROM source_data AS s

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest_records AS l
        WHERE
            s.h_customer_pk = l.h_customer_pk
            AND s.record_source = l.record_source
            AND s.hashdiff = l.hashdiff
    )
{% endif %}
