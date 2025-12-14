{{ config(
    materialized='incremental',
    unique_key='h_order_pk',
    incremental_strategy='merge',
    tags=['raw_vault', 'hub']
) }}

with src as (
    select distinct
        order_id as bk_order_id
        , {{ record_source('tpch', 'ORDERS') }} as record_source
        , sha2(coalesce(to_varchar(order_id), ''), 256) as h_order_pk
        , current_timestamp() as load_ts
    from {{ ref('stg_orders') }}
)

select *
from src as s

{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as t
        where t.h_order_pk = s.h_order_pk
    )
{% endif %}
