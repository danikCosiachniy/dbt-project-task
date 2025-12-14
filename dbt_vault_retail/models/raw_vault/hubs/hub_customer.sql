{{ config(
    materialized='incremental',
    unique_key='h_customer_pk',
    incremental_strategy='merge',
    tags=['raw_vault', 'hub']
) }}

with src as (
    select distinct
        customer_id as bk_customer_id
        , {{ record_source('tpch', 'CUSTOMER') }} as record_source
        , sha2(coalesce(to_varchar(customer_id), ''), 256) as h_customer_pk
        , current_timestamp() as load_ts
    from {{ ref('stg_customer') }}
)

select *
from src as s

{% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as t
        where t.h_customer_pk = s.h_customer_pk
    )
{% endif %}
