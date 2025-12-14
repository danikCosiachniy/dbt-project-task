{{ config(
    materialized='incremental',
    unique_key=['h_order_pk', 'h_customer_pk', 'pit_date'],
    incremental_strategy='merge',
    tags=['business_vault', 'pit']
) }}

with spine as (
    select distinct
        h_order_pk
        , h_customer_pk
        , effective_from::date as pit_date
    from {{ ref('lnk_order_customer') }}
)

select
    s.h_order_pk
    , s.h_customer_pk
    , s.pit_date
    , {{ record_source('tpch', 'ORDERS') }} as record_source
    , current_timestamp() as load_ts
from spine as s

{% if is_incremental() %}
    where
        s.pit_date > (
            select coalesce(max(t.pit_date), '1900-01-01'::date)
            from {{ this }} as t
        )
{% endif %}
