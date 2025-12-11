{{ config(materialized='table', tags=['mart', 'fact']) }}

with line as (
    select * from {{ ref('stg_lineitem') }}
)

, orders as (
    select * from {{ ref('stg_orders') }}
)

, hub_customer as (
    select
        h_customer_pk
        , bk_customer_id
    from {{ ref('hub_customer') }}
)

, base as (
    select
        l.order_id
        , l.line_number
        , l.part_id
        , l.extended_price
        , l.quantity
        , l.discount
        , o.order_date::date as order_date
        , o.customer_id
        , hc.h_customer_pk
    from line as l
    inner join orders as o on l.order_id = o.order_id
    left join hub_customer as hc on o.customer_id = hc.bk_customer_id
)

, dim_cust as (
    select * from {{ ref('dim_customer') }}
)

, dim_prod as (
    select * from {{ ref('dim_product') }}
)

, dim_d as (
    select * from {{ ref('dim_date') }}
)

select
    b.order_id

    , b.line_number
    , d.date_key as order_date_key

    , dc.customer_key
    , dp.product_key
    , b.extended_price

    , b.quantity
    , b.discount
    , sha2(
        coalesce(to_varchar(b.order_id), '') || '|'
        || coalesce(to_varchar(b.line_number), '')
        , 256
    ) as sales_key
    , b.extended_price * (1 - b.discount) as net_amount
from base as b
left join dim_d as d
    on b.order_date = d.date_key
left join dim_cust as dc
    on
        b.h_customer_pk = dc.h_customer_pk
        and b.order_date between dc.valid_from and dc.valid_to
left join dim_prod as dp
    on b.part_id = dp.part_id
