{{ config(
    materialized='incremental',
    unique_key=['h_customer_pk', 'effective_from', 'record_source'],
    incremental_strategy='merge',
    tags=['business_vault', 'sat']
) }}

with master as (
    select
        customer_id
        , segment
        , vip_flag
        , manager_id
    from {{ ref('customer_master') }}
)

, hub as (
    select
        h_customer_pk
        , bk_customer_id
    from {{ ref('hub_customer') }}
)

, customer_business_date as (
    select
        customer_id
        , min(order_date)::date as business_effective_from
    from {{ ref('stg_orders') }}
    group by customer_id
)

, src as (
    select
        h.h_customer_pk
        , m.segment
        , m.vip_flag
        , m.manager_id
        , {{ record_source('seed', 'CUSTOMER_MASTER') }} as record_source
        , coalesce(cbd.business_effective_from, '1900-01-01'::date) as effective_from
        , current_timestamp() as load_ts
        , sha2(
            coalesce(to_varchar(m.segment), '') || '|'
            || coalesce(to_varchar(m.vip_flag), '') || '|'
            || coalesce(to_varchar(m.manager_id), '')
            , 256
        ) as hashdiff
    from hub as h
    inner join master as m
        on try_to_number(h.bk_customer_id) = try_to_number(m.customer_id)
    left join customer_business_date as cbd
        on try_to_number(m.customer_id) = try_to_number(cbd.customer_id)
)

{% if is_incremental() %}
    , latest as (
        select
            h_customer_pk
            , record_source
            , hashdiff
        from {{ this }}
        qualify row_number() over (
            partition by h_customer_pk, record_source
            order by load_ts desc
        ) = 1
    )
{% endif %}

select
    s.h_customer_pk
    , s.segment
    , s.vip_flag
    , s.manager_id
    , s.record_source
    , s.effective_from
    , s.load_ts
    , s.hashdiff
from src as s

{% if is_incremental() %}
    left join latest as l
        on
            s.h_customer_pk = l.h_customer_pk
            and s.record_source = l.record_source
    where
        l.h_customer_pk is null
        or s.hashdiff != l.hashdiff
{% endif %}
