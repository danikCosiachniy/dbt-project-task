{{ config(
    materialized='incremental',
    unique_key=['h_customer_pk', 'effective_from', 'record_source'],
    incremental_strategy='merge',
    tags=['raw_vault', 'sat', 'high_volatility']
) }}

with src as (
    select
        phone
        , account_balance
        , customer_address
        , hd_customer_contact as hashdiff
        , null::timestamp as effective_to
        , 'SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER' as record_source
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
        on s.h_customer_pk = l.h_customer_pk
    where
        l.h_customer_pk is null
        or s.hashdiff != l.hashdiff
{% endif %}
