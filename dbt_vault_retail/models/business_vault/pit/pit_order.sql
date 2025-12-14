{{ config(
    materialized='incremental',
    unique_key=['h_order_pk', 'pit_date'],
    incremental_strategy='merge',
    tags=['business_vault', 'pit']
) }}

with base as (
    select distinct
        h_order_pk
        , effective_from::date as pit_date
    from {{ ref('lnk_order_customer') }}
)

, eff_sat as (
    select
        h_order_pk
        , order_status
        , effective_from
        , effective_to
    from {{ ref('eff_sat_order_status') }}
)

select
    b.h_order_pk
    , b.pit_date
    , s.order_status
    , {{ record_source('tpch', 'ORDERS') }} as record_source
    , current_timestamp() as load_ts
from base as b
left join eff_sat as s
    on
        b.h_order_pk = s.h_order_pk
        and b.pit_date between s.effective_from and s.effective_to

{% if is_incremental() %}
    where
        b.pit_date > (
            select coalesce(max(t.pit_date), '1900-01-01'::date)
            from {{ this }} as t
        )
{% endif %}
