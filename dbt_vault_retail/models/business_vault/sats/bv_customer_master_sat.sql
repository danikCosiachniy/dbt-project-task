{{ config(
    materialized='incremental',
    incremental_strategy='append',
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
        , load_ts AS hub_load_ts
    FROM {{ ref('hub_customer') }}
)

, src AS (
    SELECT
        h.h_customer_pk
        , coalesce(m.segment, 'UNKNOWN') AS segment
        , coalesce(m.vip_flag, FALSE) AS vip_flag
        , coalesce(m.manager_id, -1) AS manager_id
        , {{ record_source('seed', 'CUSTOMER_MASTER') }} AS record_source
        , cast('{{ run_started_at }}' AS timestamp_tz) AS effective_from
        , cast('{{ run_started_at }}' AS timestamp_tz) AS load_ts
        , h.hub_load_ts
        , sha2(
            coalesce(to_varchar(coalesce(m.segment, 'UNKNOWN')), '') || '|'
            || coalesce(to_varchar(coalesce(m.vip_flag, FALSE)), '') || '|'
            || coalesce(to_varchar(coalesce(m.manager_id, -1)), '') || '|'
            || {{ record_source('seed', 'CUSTOMER_MASTER') }}
            , 256
        ) AS hashdiff
    FROM hub AS h
    LEFT JOIN master AS m
        ON try_to_number(h.bk_customer_id) = try_to_number(m.customer_id)

    {% if is_incremental() %}
        WHERE h.hub_load_ts >= dateadd(
            DAY, 0
            , (SELECT coalesce(max(t.hub_load_ts), to_timestamp_tz('1900-01-01')) FROM {{ this }} AS t)
        )
    {% endif %}
)

SELECT s.*
FROM src AS s

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE
            t.h_customer_pk = s.h_customer_pk
            AND t.record_source = s.record_source
            AND t.hashdiff = s.hashdiff
    )
{% endif %}
