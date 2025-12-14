{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['h_product_pk'],
    tags=['raw_vault', 'hub']
) }}

with src as (
    select distinct
        l_partkey as bk_part_id
        , {{ record_source('tpch', 'LINEITEM') }} as record_source
        , sha2(coalesce(to_varchar(l_partkey), ''), 256) as h_product_pk
        , current_timestamp() as load_ts
        , current_date() as load_date
    from {{ source('tpch_sf1', 'LINEITEM') }}
)

select
    s.bk_part_id
    , s.h_product_pk
    , s.record_source
    , s.load_ts
    , s.load_date
from src as s

{% if is_incremental() %}
    left join {{ this }} as t
        on s.h_product_pk = t.h_product_pk
    where t.h_product_pk is null
{% endif %}
