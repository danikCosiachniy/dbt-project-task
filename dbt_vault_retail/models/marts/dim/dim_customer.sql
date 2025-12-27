{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['h_customer_pk', 'valid_from'],
    tags=['mart', 'dimension']
) }}

WITH pit_src AS (
    SELECT
        hk_customer AS h_customer_pk
        , as_of_ts
        , cast(as_of_ts AS date) AS pit_date
        , core_load_ts
        , contact_load_ts
        , master_load_ts
    FROM {{ ref('pit_customer') }}
)

{% if is_incremental() %}
    , changed_customers AS (
        SELECT DISTINCT p.h_customer_pk
        FROM pit_src AS p
        WHERE p.as_of_ts > (
            SELECT coalesce(max(t.load_ts), to_timestamp_tz('1900-01-01'))
            FROM {{ this }} AS t
        )
    )

    , pit AS (
        SELECT p.*
        FROM pit_src AS p
        INNER JOIN changed_customers AS c
            ON p.h_customer_pk = c.h_customer_pk
    )
{% else %}
, pit as (
    select * from pit_src
)
{% endif %}

, core AS (
    SELECT
        h_customer_pk
        , load_ts
        , customer_name
        , market_segment
    FROM {{ ref('sat_customer_core') }}
)

, contact AS (
    SELECT
        h_customer_pk
        , load_ts
        , phone
        , account_balance
        , customer_address
    FROM {{ ref('sat_customer_contact') }}
)

, master AS (
    SELECT
        h_customer_pk
        , load_ts
        , segment AS business_segment
        , vip_flag
        , manager_id
    FROM {{ ref('bv_customer_master_sat') }}
)

, asof_joined AS (
    SELECT
        p.h_customer_pk
        , p.pit_date
        , p.as_of_ts

        , c.customer_name
        , c.market_segment

        , ct.phone
        , ct.account_balance
        , ct.customer_address

        , m.business_segment
        , m.vip_flag
        , m.manager_id
    FROM pit AS p
    LEFT JOIN core AS c
        ON
            p.h_customer_pk = c.h_customer_pk
            AND p.core_load_ts = c.load_ts
    LEFT JOIN contact AS ct
        ON
            p.h_customer_pk = ct.h_customer_pk
            AND p.contact_load_ts = ct.load_ts
    LEFT JOIN master AS m
        ON
            p.h_customer_pk = m.h_customer_pk
            AND p.master_load_ts = m.load_ts

    QUALIFY row_number() OVER (
        PARTITION BY p.h_customer_pk, p.pit_date
        ORDER BY p.as_of_ts DESC
    ) = 1
)

, ranges AS (
    SELECT
        a.h_customer_pk
        , a.pit_date AS valid_from

        , a.customer_name
        , a.market_segment
        , a.phone
        , a.account_balance
        , a.customer_address
        , a.business_segment
        , a.vip_flag
        , a.manager_id

        , lead(a.pit_date) OVER (
            PARTITION BY a.h_customer_pk
            ORDER BY a.pit_date
        ) AS next_date
    FROM asof_joined AS a
)

, new_rows AS (
    SELECT
        r.h_customer_pk
        , r.valid_from
        , r.customer_name
        , r.market_segment
        , r.phone
        , r.account_balance
        , r.customer_address
        , r.business_segment
        , r.vip_flag
        , r.manager_id

        , sha2(
            coalesce(to_varchar(r.h_customer_pk), '') || '|'
            || coalesce(to_varchar(r.valid_from), '')
            , 256
        ) AS customer_key

        , coalesce(r.next_date, to_date('9999-12-31')) AS valid_to
        , (r.next_date IS NULL) AS is_current

        , cast('{{ run_started_at }}' AS timestamp_tz) AS load_ts
    FROM ranges AS r
)

{% if is_incremental() %}
    , to_close AS (
        SELECT
            t.h_customer_pk
            , t.valid_from
            , min(n.valid_from) AS new_valid_from
        FROM {{ this }} AS t
        INNER JOIN new_rows AS n
            ON t.h_customer_pk = n.h_customer_pk
        WHERE
            t.is_current = TRUE
            AND n.valid_from > t.valid_from
        GROUP BY
            t.h_customer_pk
            , t.valid_from
    )

    , closing_rows AS (
        SELECT
            t.h_customer_pk
            , t.valid_from
            , t.customer_name
            , t.market_segment
            , t.phone
            , t.account_balance
            , t.customer_address
            , t.business_segment
            , t.vip_flag
            , t.manager_id
            , t.customer_key

            , c.new_valid_from AS valid_to
            , FALSE AS is_current

            , cast('{{ run_started_at }}' AS timestamp_tz) AS load_ts
        FROM {{ this }} AS t
        INNER JOIN to_close AS c
            ON
                t.h_customer_pk = c.h_customer_pk
                AND t.valid_from = c.valid_from
    )
{% endif %}

{% if is_incremental() %}
    SELECT * FROM closing_rows
    UNION ALL
    SELECT * FROM new_rows
{% else %}
SELECT * FROM new_rows
{% endif %}
