{{ config(
    materialized='incremental',
    unique_key=['h_customer_pk', 'pit_date'],
    incremental_strategy='merge',
    tags=['business_vault', 'pit']
) }}

with dates as (
    select distinct effective_from::date as pit_date
    from {{ ref('sat_customer_core') }}
    union distinct
    select distinct effective_from::date as pit_date
    from {{ ref('sat_customer_contact') }}
    union distinct
    select distinct effective_from::date as pit_date
    from {{ ref('bv_customer_master_sat') }}
)

, core_latest as (
    select *
    from (
        select
            h_customer_pk
            , customer_name
            , market_segment
            , effective_from
            , row_number() over (
                partition by h_customer_pk
                order by effective_from desc
            ) as rn
        from {{ ref('sat_customer_core') }}
    )
    where rn = 1
)

, contact_latest as (
    select *
    from (
        select
            h_customer_pk
            , phone
            , account_balance
            , customer_address
            , effective_from
            , row_number() over (
                partition by h_customer_pk
                order by effective_from desc
            ) as rn
        from {{ ref('sat_customer_contact') }}
    )
    where rn = 1
)

, bv_latest as (
    select *
    from (
        select
            h_customer_pk
            , segment
            , vip_flag
            , manager_id
            , effective_from
            , row_number() over (
                partition by h_customer_pk
                order by effective_from desc
            ) as rn
        from {{ ref('bv_customer_master_sat') }}
    )
    where rn = 1
)

select
    h.h_customer_pk
    , d.pit_date
    , c.customer_name
    , c.market_segment
    , ct.phone
    , ct.account_balance
    , ct.customer_address
    , bv.segment as business_segment
    , bv.vip_flag
    , bv.manager_id
    , 'BUSINESS_VAULT.PIT_CUSTOMER' as record_source
    , current_timestamp() as load_ts
from {{ ref('hub_customer') }} as h
cross join dates as d
left join core_latest as c on h.h_customer_pk = c.h_customer_pk
left join contact_latest as ct on h.h_customer_pk = ct.h_customer_pk
left join bv_latest as bv on h.h_customer_pk = bv.h_customer_pk

{% if is_incremental() %}
    where d.pit_date > (
        select coalesce(max(t.pit_date), '1900-01-01') from {{ this }} as t
    )
{% endif %}
