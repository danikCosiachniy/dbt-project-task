{{ config(materialized='table', tags=['mart', 'dimension']) }}

with hub_product as (
    select
        h_product_pk
        , bk_part_id as part_id
    from {{ ref('hub_product') }}
)

, lnk as (
    select
        l_order_lineitem_pk
        , h_product_pk
    from {{ ref('lnk_order_lineitem') }}
)

, sat as (
    select
        l_order_lineitem_pk
        , extended_price
        , load_ts
        , hashdiff
    from {{ ref('sat_order_lineitem_measures') }}
    qualify row_number() over (
        partition by l_order_lineitem_pk
        order by load_ts desc
    ) = 1
)

, parts as (
    select
        hp.part_id
        , hp.h_product_pk
        , min(s.extended_price) as min_price
        , max(s.extended_price) as max_price
        , avg(s.extended_price) as avg_price
    from sat as s
    inner join lnk as l
        on s.l_order_lineitem_pk = l.l_order_lineitem_pk
    inner join hub_product as hp
        on l.h_product_pk = hp.h_product_pk
    group by
        hp.part_id
        , hp.h_product_pk
)

select
    part_id
    , min_price
    , max_price
    , avg_price
    , sha2(coalesce(to_varchar(h_product_pk), ''), 256) as product_key
from parts
