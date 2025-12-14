{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_order_lineitem_pk'],
    tags=['raw_vault', 'link']
) }}

with src as (
    select
        l_orderkey as order_id
        , l_linenumber as line_number
        , l_partkey as part_id
        , {{ record_source('tpch', 'LINEITEM') }} as record_source
        , sha2(coalesce(to_varchar(l_orderkey), ''), 256) as h_order_pk
        , sha2(coalesce(to_varchar(l_partkey), ''), 256) as h_product_pk
        , sha2(
            coalesce(to_varchar(l_orderkey), '')
            || '|'
            || coalesce(to_varchar(l_linenumber), '')
            || '|'
            || coalesce(to_varchar(l_partkey), '')
            , 256
        ) as l_order_lineitem_pk
        , current_timestamp() as load_ts
        , current_date() as load_date
    from {{ source('tpch_sf1', 'LINEITEM') }}
)

select
    s.l_order_lineitem_pk
    , s.h_order_pk
    , s.h_product_pk
    , s.record_source
    , s.load_ts
    , s.load_date
from src as s

{% if is_incremental() %}
    left join {{ this }} as t
        on s.l_order_lineitem_pk = t.l_order_lineitem_pk
    where t.l_order_lineitem_pk is null
{% endif %}
