{{ config(materialized='table', tags=['mart', 'fact']) }}

with lineitem_sat as (

    select
        l_order_lineitem_pk
        , extended_price
        , quantity
        , discount
    from {{ ref('sat_order_lineitem_measures') }}
    qualify row_number() over (
        partition by l_order_lineitem_pk
        order by load_ts desc
    ) = 1

)

, lineitem_link as (

    select
        l_order_lineitem_pk
        , h_order_pk
        , h_product_pk
    from {{ ref('lnk_order_lineitem') }}

)

, hub_product as (

    select
        h_product_pk
        , bk_part_id as part_id
    from {{ ref('hub_product') }}

)

, order_customer as (

    select
        h_order_pk
        , h_customer_pk
        , pit_date
    from {{ ref('pit_order_customer') }}

)

, order_pit as (

    select
        h_order_pk
        , pit_date
    from {{ ref('pit_order') }}

)

, base as (

    select
        l.l_order_lineitem_pk
        , oc.h_customer_pk
        , l.h_product_pk
        , hp.part_id
        , op.pit_date as order_date
        , s.extended_price
        , s.quantity
        , s.discount
    from lineitem_link as l
    inner join lineitem_sat as s
        on l.l_order_lineitem_pk = s.l_order_lineitem_pk
    inner join order_pit as op
        on l.h_order_pk = op.h_order_pk
    left join order_customer as oc
        on
            l.h_order_pk = oc.h_order_pk
            and op.pit_date = oc.pit_date
    left join hub_product as hp
        on l.h_product_pk = hp.h_product_pk

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
    b.l_order_lineitem_pk as sales_key
    , d.date_key as order_date_key

    , dc.customer_key
    , dp.product_key
    , b.extended_price

    , b.quantity
    , b.discount
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
