{{ config(materialized='table', tags=['mart', 'dimension']) }}

WITH pit AS (
    SELECT
        h_customer_pk
        , pit_date
        , customer_name
        , market_segment
        , phone
        , account_balance
        , customer_address
        , business_segment
        , vip_flag
        , manager_id
    FROM {{ ref('pit_customer') }}
)

, ranges AS (
    SELECT
        h_customer_pk
        , pit_date AS valid_from
        , customer_name
        , market_segment
        , phone
        , account_balance
        , customer_address
        , business_segment
        , vip_flag
        , manager_id
        , lead(pit_date) OVER (
            PARTITION BY h_customer_pk
            ORDER BY pit_date
        ) AS next_date
    FROM pit
)

, final AS (
    SELECT
        h_customer_pk
        , valid_from
        , customer_name
        , market_segment
        , phone

        , account_balance
        , customer_address
        , business_segment
        , vip_flag
        , manager_id
        , sha2(
            coalesce(to_varchar(h_customer_pk), '') || '|'
            || coalesce(to_varchar(valid_from), '')
            , 256
        ) AS customer_key
        , coalesce(next_date - INTERVAL '1 day', '9999-12-31'::date) AS valid_to
        , coalesce(next_date IS NULL, FALSE) AS is_current
    FROM ranges
)

SELECT * FROM final
