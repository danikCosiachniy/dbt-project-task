{{ config(materialized='view', tags=['staging']) }}

SELECT
    l_orderkey AS order_id
    , l_linenumber AS line_number
    , l_partkey AS part_id
    , l_suppkey AS supplier_id
    , l_quantity AS quantity
    , l_extendedprice AS extended_price
    , l_discount AS discount
    , l_tax AS tax
    , l_returnflag AS return_flag
    , l_linestatus AS line_status
    , l_shipdate AS ship_date
    , l_commitdate AS commit_date
    , l_receiptdate AS receipt_date
    , l_shipinstruct AS ship_instruct
    , l_shipmode AS ship_mode
    , l_comment AS line_comment
    , {{ record_source('tpch', 'LINEITEM') }} AS record_source
    , current_timestamp() AS load_ts
    , sha2(coalesce(to_varchar(l_orderkey), ''), 256) AS h_order_pk
    , sha2(coalesce(to_varchar(l_partkey), ''), 256) AS h_product_pk
    , sha2(
        coalesce(to_varchar(l_orderkey), '') || '|'
        || coalesce(to_varchar(l_linenumber), '') || '|'
        || coalesce(to_varchar(l_partkey), '')
        , 256
    ) AS l_order_lineitem_pk
    , sha2(
        coalesce(to_varchar(l_quantity), '') || '|'
        || coalesce(to_varchar(l_extendedprice), '') || '|'
        || coalesce(to_varchar(l_discount), '') || '|'
        || coalesce(to_varchar(l_tax), '') || '|'
        || coalesce(to_varchar(l_shipdate), '') || '|'
        || coalesce(to_varchar(l_commitdate), '') || '|'
        || coalesce(to_varchar(l_receiptdate), '') || '|'
        || {{ record_source('tpch', 'LINEITEM') }}
        , 256
    ) AS hashdiff
FROM {{ source('tpch_sf1', 'LINEITEM') }}
