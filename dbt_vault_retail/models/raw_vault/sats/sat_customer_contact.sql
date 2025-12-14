{{ config(
    materialized='incremental',
    unique_key=['h_customer_pk', 'load_ts', 'record_source'],
    incremental_strategy='merge',
    tags=['raw_vault', 'sat', 'high_volatility']
) }}

with customer_business_date as (
    select
        customer_id
        , min(order_date)::timestamp as business_effective_from
    from {{ ref('stg_orders') }}
    group by customer_id
)

, src as (
    select
        c.phone
        , c.account_balance
        , c.customer_address
        , c.hd_customer_contact as hashdiff
        , null::timestamp as effective_to
        , {{ record_source('tpch', 'CUSTOMER') }} as record_source
        , sha2(coalesce(to_varchar(c.customer_id), ''), 256) as h_customer_pk
        , current_timestamp() as load_ts
        , coalesce(cbd.business_effective_from, current_timestamp()) as effective_from
    from {{ ref('stg_customer') }} as c
    left join customer_business_date as cbd
        on c.customer_id = cbd.customer_id
)

{% if is_incremental() %}
    , latest_records as (
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
    s.phone
    , s.account_balance
    , s.customer_address
    , s.effective_to
    , s.record_source
    , s.h_customer_pk
    , s.effective_from
    , s.load_ts
    , s.hashdiff
from src as s
{% if is_incremental() %}
    left join latest_records as l
        on
            s.h_customer_pk = l.h_customer_pk
            and s.record_source = l.record_source
    where
        l.h_customer_pk is null
        or s.hashdiff != l.hashdiff
{% endif %}
