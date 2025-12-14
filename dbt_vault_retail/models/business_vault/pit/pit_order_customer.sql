{{ config(
    materialized='incremental',
    unique_key=['h_order_pk', 'h_customer_pk', 'pit_date'],
    incremental_strategy='merge',
    tags=['business_vault', 'pit']
) }}

with lnk as (
    select
        h_order_pk
        , h_customer_pk
    from {{ ref('lnk_order_customer') }}
)

, orders as (
    select
        order_date::date as pit_date
        , sha2(coalesce(to_varchar(order_id), ''), 256) as h_order_pk
    from {{ ref('stg_orders') }}
)

, base as (
    select
        l.h_order_pk
        , l.h_customer_pk
        , o.pit_date
    from lnk as l
    inner join orders as o
        on l.h_order_pk = o.h_order_pk
)

, cust_pit as (
    select * from {{ ref('pit_customer') }}
)

, order_pit as (
    select * from {{ ref('pit_order') }}
)

select
    b.h_order_pk
    , b.h_customer_pk
    , b.pit_date
    , cp.customer_name
    , cp.market_segment
    , cp.phone
    , cp.account_balance
    , cp.customer_address
    , cp.business_segment
    , cp.vip_flag
    , op.order_status
    , 'BUSINESS_VAULT.PIT_ORDER_CUSTOMER' as record_source
    , current_timestamp() as load_ts
from base as b
left join cust_pit as cp on b.h_customer_pk = cp.h_customer_pk and b.pit_date = cp.pit_date
left join order_pit as op on b.h_order_pk = op.h_order_pk and b.pit_date = op.pit_date

{% if is_incremental() %}
    where b.pit_date > (
        select coalesce(max(t.pit_date), '1900-01-01') from {{ this }} as t
    )
{% endif %}
