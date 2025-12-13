{{ config(materialized='view', tags=['staging']) }}

select
    c_custkey as customer_id
    , c_name as customer_name
    , c_address as customer_address
    , c_nationkey as nation_id
    , c_phone as phone
    , c_acctbal as account_balance
    , c_mktsegment as market_segment
    , c_comment as customer_comment
    , sha2(
        coalesce(to_varchar(c_name), '') || '|'
        || coalesce(to_varchar(c_mktsegment), '')
        , 256
    ) as hd_customer_core
    , sha2(
        coalesce(to_varchar(c_phone), '') || '|'
        || coalesce(to_varchar(c_acctbal), '') || '|'
        || coalesce(to_varchar(c_address), '')
        , 256
    ) as hd_customer_contact

from {{ source('tpch_sf1', 'CUSTOMER') }}
