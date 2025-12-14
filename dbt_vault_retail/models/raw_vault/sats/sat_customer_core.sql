{{ config(
    materialized='incremental',
    unique_key=['h_customer_pk', 'effective_from', 'record_source'],
    incremental_strategy='merge',
    tags=['raw_vault', 'sat', 'low_volatility']
) }}

with source_data as (
    select
        customer_name
        , market_segment
        , hd_customer_core as hashdiff
        , null::timestamp as effective_to
        , {{ record_source('tpch', 'CUSTOMER') }} as record_source
        , sha2(coalesce(to_varchar(customer_id), ''), 256) as h_customer_pk
        , current_timestamp() as load_ts
        , {% if is_incremental() %} current_timestamp() {% else %} '1990-01-01'::timestamp {% endif %} as effective_from
    from {{ ref('stg_customer') }}
)

{% if is_incremental() %}
    , latest_records as (
        select
            h_customer_pk
            , hashdiff
        from {{ this }}
        qualify row_number() over (
            partition by h_customer_pk
            order by effective_from desc
        ) = 1
    )
{% endif %}

select
    s.customer_name
    , s.market_segment
    , s.effective_to
    , s.record_source
    , s.h_customer_pk
    , s.effective_from
    , s.load_ts
    , s.hashdiff
from source_data as s
{% if is_incremental() %}
    left join latest_records as l
        on s.h_customer_pk = l.h_customer_pk
    where
        l.h_customer_pk is null
        or s.hashdiff != l.hashdiff
{% endif %}
