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
FROM {{ source('tpch_sf1', 'ORDERS') }}
