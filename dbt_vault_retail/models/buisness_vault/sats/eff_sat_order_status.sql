{{ config(
    materialized='incremental',
    unique_key=['h_order_pk', 'effective_from', 'record_source'],
    incremental_strategy='merge',
    tags=['business_vault', 'sat', 'effectivity']
) }}

with src as (
    select
        order_id
        , order_status
        , order_date::date as status_change_date
        , 'SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS' as record_source
        , sha2(coalesce(to_varchar(order_id), ''), 256) as h_order_pk
    from {{ ref('stg_orders') }}
)

, ordered as (
    select
        h_order_pk
        , order_status
        , status_change_date as effective_from
        , record_source
        , lead(status_change_date) over (
            partition by h_order_pk
            order by status_change_date
        ) as next_change_date
    from src
)

, final as (
    select
        h_order_pk
        , order_status
        , effective_from
        , record_source
        , coalesce(
            next_change_date - interval '1 day'
            , '9999-12-31'::date
        ) as effective_to
        , current_timestamp() as load_ts
    from ordered
)

select *
from final

{% if is_incremental() %}
where effective_from >
      (select coalesce(max(effective_from), '1900-01-01'::date) from {{ this }})
{% endif %}
