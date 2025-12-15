{{ config(
    materialized='incremental',
    unique_key=['h_customer_pk', 'effective_from', 'record_source'],
    incremental_strategy='merge',
    tags=['business_vault', 'sat']
) }}

WITH master AS (
    SELECT
        customer_id
        , segment
        , vip_flag
        , manager_id
    FROM {{ ref('customer_master') }}
)

, hub AS (
    SELECT
        h_customer_pk
        , bk_customer_id
    FROM {{ ref('hub_customer') }}
)

, customer_business_date AS (
    SELECT
        customer_id
        , min(order_date)::date AS business_effective_from
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)

, src AS (
    SELECT
        h.h_customer_pk
        , m.segment
        , m.vip_flag
        , m.manager_id
        , {{ record_source('seed', 'CUSTOMER_MASTER') }} AS record_source
        , coalesce(cbd.business_effective_from, '1900-01-01'::date) AS effective_from
        , current_timestamp() AS load_ts
        , sha2(
            coalesce(to_varchar(m.segment), '') || '|'
            || coalesce(to_varchar(m.vip_flag), '') || '|'
            || coalesce(to_varchar(m.manager_id), '')
            , 256
        ) AS hashdiff
    FROM hub AS h
    INNER JOIN master AS m
        ON try_to_number(h.bk_customer_id) = try_to_number(m.customer_id)
    LEFT JOIN customer_business_date AS cbd
        ON try_to_number(m.customer_id) = try_to_number(cbd.customer_id)
)

{% if is_incremental() %}
    , latest AS (
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
    s.h_customer_pk
    , s.segment
    , s.vip_flag
    , s.manager_id
    , s.record_source
    , s.effective_from
    , s.load_ts
    , s.hashdiff
FROM src AS s

{% if is_incremental() %}
    LEFT JOIN latest AS l
        ON
            s.h_customer_pk = l.h_customer_pk
            AND s.record_source = l.record_source
    WHERE
        l.h_customer_pk IS NULL
        OR s.hashdiff != l.hashdiff
{% endif %}
