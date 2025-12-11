{{ config(materialized='table', tags=['mart', 'dimension']) }}

with parts as (
    select
        part_id
        , min(extended_price) as min_price
        , max(extended_price) as max_price
        , avg(extended_price) as avg_price
    from {{ ref('stg_lineitem') }}
    group by part_id
)

select
    part_id
    , min_price
    , max_price
    , avg_price
    , sha2(coalesce(to_varchar(part_id), ''), 256) as product_key
from parts
