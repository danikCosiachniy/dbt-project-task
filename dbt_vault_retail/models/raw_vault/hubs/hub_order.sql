{{ config(
    materialized='incremental',
    unique_key=['h_order_pk', 'load_date', 'record_source'],
    incremental_strategy='merge',
    tags=['raw_vault', 'hub']
) }}

with src as (
    select
        order_id as bk_order_id
        , 'SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS' as record_source
        , sha2(coalesce(to_varchar(order_id), ''), 256) as h_order_pk
        , current_date() as load_date
        , current_timestamp() as load_ts
    from {{ ref('stg_orders') }}
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
