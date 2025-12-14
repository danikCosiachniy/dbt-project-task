{{ config(
    materialized='incremental',
    unique_key='l_order_customer_pk',
    incremental_strategy='merge',
    tags=['raw_vault', 'link']
) }}

with src as (
    select
        o.order_date as effective_from
        , {{ record_source('tpch', 'ORDERS') }} as record_source
        , sha2(
            coalesce(to_varchar(o.order_id), '') || '|'
            || coalesce(to_varchar(o.customer_id), '')
            , 256
        ) as l_order_customer_pk
        , sha2(coalesce(to_varchar(o.order_id), ''), 256) as h_order_pk
        , sha2(coalesce(to_varchar(o.customer_id), ''), 256) as h_customer_pk
        , current_timestamp() as load_ts
    from {{ ref('stg_orders') }} as o
)

select *
from src as s

{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as t
        where t.l_order_customer_pk = s.l_order_customer_pk
    )
{% endif %}
