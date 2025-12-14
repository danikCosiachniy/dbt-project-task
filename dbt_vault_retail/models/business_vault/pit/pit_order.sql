{{ config(
    materialized='incremental',
    unique_key=['h_order_pk', 'pit_date'],
    incremental_strategy='merge',
    tags=['business_vault', 'pit']
) }}

with orders as (
    select
        order_date::date as pit_date
        , sha2(coalesce(to_varchar(order_id), ''), 256) as h_order_pk
    from {{ ref('stg_orders') }}
)

, eff_sat as (
    select
        h_order_pk
        , order_status
        , effective_from
        , effective_to
    from {{ ref('eff_sat_order_status') }}
)

, final as (
    select
        o.h_order_pk
        , o.pit_date
        , s.order_status
        , 'BUSINESS_VAULT.PIT_ORDER' as record_source
        , current_timestamp() as load_ts
    from orders as o
    left join eff_sat as s
        on
            o.h_order_pk = s.h_order_pk
            and o.pit_date between s.effective_from and s.effective_to
)

select * from final

{% if is_incremental() %}
    where pit_date > (
        select coalesce(max(t.pit_date), '1900-01-01'::date)
        from {{ this }} as t
    )
{% endif %}
