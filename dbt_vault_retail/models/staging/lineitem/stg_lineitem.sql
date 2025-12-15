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
FROM {{ source('tpch_sf1', 'LINEITEM') }}
