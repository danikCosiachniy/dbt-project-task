{{ config(materialized='view', tags=['staging']) }}

SELECT
    o_orderkey AS order_id
    , o_custkey AS customer_id
    , o_orderstatus AS order_status
    , o_totalprice AS total_price
    , o_orderdate AS order_date
    , o_orderpriority AS order_priority
    , o_clerk AS clerk
    , o_shippriority AS ship_priority
    , o_comment AS order_comment
    , {{ record_source('tpch', 'ORDERS') }} AS record_source
    , sha2(coalesce(to_varchar(o_orderkey), ''), 256) AS h_order_pk
    , sha2(coalesce(to_varchar(o_custkey), ''), 256) AS h_customer_pk
    , sha2(
        coalesce(to_varchar(o_orderstatus), '') || '|'
        || coalesce(to_varchar(o_totalprice), '') || '|'
        || coalesce(to_varchar(o_orderdate), '') || '|'
        || coalesce(to_varchar(o_orderpriority), '') || '|'
        || coalesce(to_varchar(o_clerk), '') || '|'
        || coalesce(to_varchar(o_shippriority), '') || '|'
        || coalesce(to_varchar(o_comment), '') || '|'
        || {{ record_source('tpch', 'ORDERS') }}
        , 256
    ) AS hashdiff
    , sha2(
        coalesce(to_varchar(o_orderkey), '') || '|'
        || coalesce(to_varchar(o_custkey), '')
        , 256
    ) AS l_order_customer_pk
    , cast('{{ run_started_at }}' AS timestamp_tz) AS load_ts
FROM {{ source('tpch_sf1', 'ORDERS') }}
