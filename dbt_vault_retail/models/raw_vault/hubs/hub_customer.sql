{{ config(
    materialized='incremental',
    unique_key=['h_customer_pk', 'load_date', 'record_source'],
    incremental_strategy='merge',
    tags=['raw_vault', 'hub']
) }}

with src as (
    select
        customer_id as bk_customer_id
        , {{ record_source('tpch', 'CUSTOMER') }} as record_source
        , sha2(coalesce(to_varchar(customer_id), ''), 256) as h_customer_pk
        , current_date() as load_date
        , current_timestamp() as load_ts
    from {{ ref('stg_customer') }}
)

select *
from src

{% if is_incremental() %}
    where
        load_date
        > (
            select coalesce(max(t.load_date), '1900-01-01'::date)
            from {{ this }} as t
        )
{% endif %}
