{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_order_lineitem_pk', 'load_ts', 'record_source'],
    tags=['raw_vault', 'sat']
) }}

with src as (
    select
        sha2(
            coalesce(to_varchar(l_orderkey), '') || '|'
            || coalesce(to_varchar(l_linenumber), '') || '|'
            || coalesce(to_varchar(l_partkey), '')
            , 256
        ) as l_order_lineitem_pk

        , l_quantity as quantity
        , l_extendedprice as extended_price
        , l_discount as discount
        , l_tax as tax
        , l_shipdate as ship_date
        , l_commitdate as commit_date
        , l_receiptdate as receipt_date

        , sha2(
            coalesce(to_varchar(l_quantity), '') || '|'
            || coalesce(to_varchar(l_extendedprice), '') || '|'
            || coalesce(to_varchar(l_discount), '') || '|'
            || coalesce(to_varchar(l_tax), '') || '|'
            || coalesce(to_varchar(l_shipdate), '') || '|'
            || coalesce(to_varchar(l_commitdate), '') || '|'
            || coalesce(to_varchar(l_receiptdate), '')
            , 256
        ) as hashdiff

        , {{ record_source('tpch', 'LINEITEM') }} as record_source
        , current_timestamp() as load_ts
        , current_date() as load_date
        , cast(null as timestamp) as effective_to
    from {{ source('tpch_sf1', 'LINEITEM') }}
)

{% if is_incremental() %}
, latest as (
    select
        l_order_lineitem_pk
        , record_source
        , hashdiff
    from {{ this }}
    qualify row_number() over (
        partition by l_order_lineitem_pk, record_source
        order by load_ts desc
    ) = 1
)
{% endif %}

select
    s.l_order_lineitem_pk
    , s.quantity
    , s.extended_price
    , s.discount
    , s.tax
    , s.ship_date
    , s.commit_date
    , s.receipt_date
    , s.hashdiff
    , s.record_source
    , s.load_ts
    , s.load_date
    , s.effective_to
from src as s

{% if is_incremental() %}
left join latest as l
    on s.l_order_lineitem_pk = l.l_order_lineitem_pk
    and s.record_source = l.record_source
where l.l_order_lineitem_pk is null or s.hashdiff != l.hashdiff
{% endif %}
