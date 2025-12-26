{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['business_vault', 'pit']
) }}

WITH as_of AS (
    SELECT DISTINCT
        h_order_pk
        , h_customer_pk
        , record_source
        , load_ts AS as_of_ts
    FROM {{ ref('lnk_order_customer') }}

    {% if is_incremental() %}
        WHERE load_ts > (
            SELECT coalesce(max(t.as_of_ts), to_timestamp_tz('1900-01-01'))
            FROM {{ this }} AS t
        )
    {% endif %}
)

, pit AS (
    SELECT
        a.h_order_pk
        , a.h_customer_pk
        , a.record_source
        , a.as_of_ts
        , l.load_ts AS link_load_ts
        , row_number() OVER (
            PARTITION BY a.h_order_pk, a.h_customer_pk, a.record_source, a.as_of_ts
            ORDER BY l.load_ts DESC
        ) AS rn
    FROM as_of AS a
    LEFT JOIN {{ ref('lnk_order_customer') }} AS l
        ON
            a.h_order_pk = l.h_order_pk
            AND a.h_customer_pk = l.h_customer_pk
            AND a.record_source = l.record_source
            AND a.as_of_ts >= l.load_ts
    QUALIFY rn = 1
)

SELECT
    p.h_order_pk
    , p.h_customer_pk
    , p.record_source
    , p.as_of_ts
    , p.link_load_ts
    , cast('{{ run_started_at }}' AS timestamp_tz) AS load_ts
FROM pit AS p

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE
            t.h_order_pk = p.h_order_pk
            AND t.h_customer_pk = p.h_customer_pk
            AND t.record_source = p.record_source
            AND t.as_of_ts = p.as_of_ts
    )
{% endif %}
