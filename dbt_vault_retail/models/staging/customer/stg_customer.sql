{{ config(materialized='view', tags=['staging']) }}

SELECT
    c_custkey AS customer_id
    , c_name AS customer_name
    , c_address AS customer_address
    , c_nationkey AS nation_id
    , c_phone AS phone
    , c_acctbal AS account_balance
    , c_mktsegment AS market_segment
    , c_comment AS customer_comment
    , sha2(
        coalesce(to_varchar(c_name), '') || '|'
        || coalesce(to_varchar(c_mktsegment), '')
        , 256
    ) AS hd_customer_core
    , sha2(
        coalesce(to_varchar(c_phone), '') || '|'
        || coalesce(to_varchar(c_acctbal), '') || '|'
        || coalesce(to_varchar(c_address), '')
        , 256
    ) AS hd_customer_contact

FROM {{ source('tpch_sf1', 'CUSTOMER') }}
