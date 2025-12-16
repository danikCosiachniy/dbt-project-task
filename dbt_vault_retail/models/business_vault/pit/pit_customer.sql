{{ config(
    materialized='incremental',
    unique_key=['h_customer_pk', 'pit_date'],
    incremental_strategy='merge',
    tags=['business_vault', 'pit']
) }}

WITH spine AS (
    SELECT DISTINCT
        l.h_customer_pk
        , l.effective_from::date AS pit_date
    FROM {{ ref('lnk_order_customer') }} AS l
)

, core_asof AS (
    SELECT
        sp.h_customer_pk
        , sp.pit_date
        , s.customer_name
        , s.market_segment
        , s.load_ts
    FROM spine AS sp
    LEFT JOIN {{ ref('sat_customer_core') }} AS s
        ON sp.h_customer_pk = s.h_customer_pk
    QUALIFY row_number() OVER (
        PARTITION BY sp.h_customer_pk, sp.pit_date
        ORDER BY s.load_ts DESC
    ) = 1
)

, contact_asof AS (
    SELECT
        sp.h_customer_pk
        , sp.pit_date
        , s.phone
        , s.account_balance
        , s.customer_address
        , s.load_ts
    FROM spine AS sp
    LEFT JOIN {{ ref('sat_customer_contact') }} AS s
        ON sp.h_customer_pk = s.h_customer_pk
    QUALIFY row_number() OVER (
        PARTITION BY sp.h_customer_pk, sp.pit_date
        ORDER BY s.load_ts DESC
    ) = 1
)

, bv_asof AS (
    SELECT
        sp.h_customer_pk
        , sp.pit_date
        , s.segment
        , s.vip_flag
        , s.manager_id
        , s.effective_from
    FROM spine AS sp
    LEFT JOIN {{ ref('bv_customer_master_sat') }} AS s
        ON
            sp.h_customer_pk = s.h_customer_pk
            AND s.effective_from::date <= sp.pit_date
    QUALIFY row_number() OVER (
        PARTITION BY sp.h_customer_pk, sp.pit_date
        ORDER BY s.effective_from DESC
    ) = 1
)

SELECT
    sp.h_customer_pk
    , sp.pit_date
    , c.customer_name
    , c.market_segment
    , ct.phone
    , ct.account_balance
    , ct.customer_address
    , coalesce(bv.segment, 'UNKNOWN') AS business_segment
    , coalesce(bv.vip_flag, FALSE) AS vip_flag
    , coalesce(bv.manager_id, -1) AS manager_id
    , {{ record_source('tpch', 'CUSTOMER') }} AS record_source
    , current_timestamp() AS load_ts
FROM spine AS sp
LEFT JOIN core_asof AS c
    ON
        sp.h_customer_pk = c.h_customer_pk
        AND sp.pit_date = c.pit_date
LEFT JOIN contact_asof AS ct
    ON
        sp.h_customer_pk = ct.h_customer_pk
        AND sp.pit_date = ct.pit_date
LEFT JOIN bv_asof AS bv
    ON
        sp.h_customer_pk = bv.h_customer_pk
        AND sp.pit_date = bv.pit_date

{% if is_incremental() %}
    WHERE
        sp.pit_date > (
            SELECT coalesce(max(t.pit_date), '1900-01-01'::date)
            FROM {{ this }} AS t
        )
{% endif %}
