{{ config(
    materialized='incremental',
    unique_key=['h_customer_pk', 'effective_from', 'record_source'],
    incremental_strategy='merge',
    tags=['business_vault', 'sat']
) }}

with master as (
    select
        customer_id
        , segment
        , vip_flag
        , manager_id
    from {{ ref('customer_master') }}
)

, hub as (
    select
        h_customer_pk
        , bk_customer_id
    from {{ ref('hub_customer') }}
)

select
    h.h_customer_pk
    , m.segment
    , m.vip_flag
    , m.manager_id
    , 'SEED.CUSTOMER_MASTER' as record_source
    , current_timestamp() as effective_from
    , current_timestamp() as load_ts
from hub as h
inner join master as m
    on h.bk_customer_id = m.customer_id

{% if is_incremental() %}
    where effective_from > (
        select coalesce(max(t.effective_from), '1900-01-01'::timestamp_ntz)
        from {{ this }} as t
    )
{% endif %}
