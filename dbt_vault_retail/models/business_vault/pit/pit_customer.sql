{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['business_vault', 'pit']
) }}

WITH core AS (
    SELECT
        h_customer_pk AS hk_customer
        , load_ts
    FROM {{ ref('sat_customer_core') }}
)

, contact AS (
    SELECT
        h_customer_pk AS hk_customer
        , load_ts
    FROM {{ ref('sat_customer_contact') }}
)

, master AS (
    SELECT
        h_customer_pk AS hk_customer
        , load_ts
    FROM {{ ref('bv_customer_master_sat') }}
)

, as_of_raw AS (
    SELECT
        hk_customer
        , load_ts AS as_of_ts
    FROM core
    UNION ALL
    SELECT
        hk_customer
        , load_ts AS as_of_ts
    FROM contact
    UNION ALL
    SELECT
        hk_customer
        , load_ts AS as_of_ts
    FROM master
)

, as_of AS (
    SELECT DISTINCT
        hk_customer
        , as_of_ts
    FROM as_of_raw

    {% if is_incremental() %}
        WHERE as_of_ts > (
            SELECT coalesce(max(t.as_of_ts), to_timestamp_tz('1900-01-01'))
            FROM {{ this }} AS t
        )
    {% endif %}
)

, core_pick AS (
    SELECT
        a.hk_customer
        , a.as_of_ts
        , c.load_ts AS core_load_ts
        , row_number() OVER (
            PARTITION BY a.hk_customer, a.as_of_ts
            ORDER BY c.load_ts DESC
        ) AS rn
    FROM as_of AS a
    LEFT JOIN core AS c
        ON
            a.hk_customer = c.hk_customer
            AND a.as_of_ts >= c.load_ts
    QUALIFY rn = 1
)

, contact_pick AS (
    SELECT
        a.hk_customer
        , a.as_of_ts
        , c.load_ts AS contact_load_ts
        , row_number() OVER (
            PARTITION BY a.hk_customer, a.as_of_ts
            ORDER BY c.load_ts DESC
        ) AS rn
    FROM as_of AS a
    LEFT JOIN contact AS c
        ON
            a.hk_customer = c.hk_customer
            AND a.as_of_ts >= c.load_ts
    QUALIFY rn = 1
)

, master_pick AS (
    SELECT
        a.hk_customer
        , a.as_of_ts
        , m.load_ts AS master_load_ts
        , row_number() OVER (
            PARTITION BY a.hk_customer, a.as_of_ts
            ORDER BY m.load_ts DESC
        ) AS rn
    FROM as_of AS a
    LEFT JOIN master AS m
        ON
            a.hk_customer = m.hk_customer
            AND a.as_of_ts >= m.load_ts
    QUALIFY rn = 1
)

SELECT
    a.hk_customer
    , a.as_of_ts
    , cp.core_load_ts
    , ctp.contact_load_ts
    , mp.master_load_ts
    , cast('{{ run_started_at }}' AS timestamp_tz) AS load_ts
FROM as_of AS a
LEFT JOIN core_pick AS cp
    ON
        a.hk_customer = cp.hk_customer
        AND a.as_of_ts = cp.as_of_ts
LEFT JOIN contact_pick AS ctp
    ON
        a.hk_customer = ctp.hk_customer
        AND a.as_of_ts = ctp.as_of_ts
LEFT JOIN master_pick AS mp
    ON
        a.hk_customer = mp.hk_customer
        AND a.as_of_ts = mp.as_of_ts

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE
            t.hk_customer = a.hk_customer
            AND t.as_of_ts = a.as_of_ts
    )
{% endif %}
