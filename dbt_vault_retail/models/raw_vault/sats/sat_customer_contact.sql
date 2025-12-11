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
        , null::timestamp as effective_to
        , 'SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER' as record_source
        , sha2(coalesce(to_varchar(customer_id), ''), 256) as h_customer_pk
        , current_timestamp() as effective_from
        , current_timestamp() as load_ts
    from {{ ref('stg_customer') }}
)

select *
from src

{% if is_incremental() %}
    where
        effective_from
        > (
            select
                coalesce(
                    max(t.effective_from)
                    , '1900-01-01'::timestamp_ntz
                )
            from {{ this }} as t
        )
{% endif %}
