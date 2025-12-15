{{ config(
    materialized='incremental',
    unique_key=['h_customer_pk', 'pit_date'],
    incremental_strategy='merge',
    tags=['business_vault', 'pit']
) }}

with spine as (
    select distinct
        l.h_customer_pk
        , l.effective_from::date as pit_date
    from {{ ref('lnk_order_customer') }} as l
)

, core_asof as (
    select
        sp.h_customer_pk
        , sp.pit_date
        , s.customer_name
        , s.market_segment
        , s.effective_from
    from spine as sp
    left join {{ ref('sat_customer_core') }} as s
        on
            sp.h_customer_pk = s.h_customer_pk
            and s.effective_from::date <= sp.pit_date
    qualify row_number() over (
        partition by sp.h_customer_pk, sp.pit_date
        order by s.effective_from desc
    ) = 1
)

, contact_asof as (
    select
        sp.h_customer_pk
        , sp.pit_date
        , s.phone
        , s.account_balance
        , s.customer_address
        , s.effective_from
    from spine as sp
    left join {{ ref('sat_customer_contact') }} as s
        on
            sp.h_customer_pk = s.h_customer_pk
            and s.effective_from::date <= sp.pit_date
    qualify row_number() over (
        partition by sp.h_customer_pk, sp.pit_date
        order by s.effective_from desc
    ) = 1
)

, bv_asof as (
    select
        sp.h_customer_pk
        , sp.pit_date
        , s.segment
        , s.vip_flag
        , s.manager_id
        , s.effective_from
    from spine as sp
    left join {{ ref('bv_customer_master_sat') }} as s
        on
            sp.h_customer_pk = s.h_customer_pk
            and s.effective_from::date <= sp.pit_date
    qualify row_number() over (
        partition by sp.h_customer_pk, sp.pit_date
        order by s.effective_from desc
    ) = 1
)

select
    sp.h_customer_pk
    , sp.pit_date
    , c.customer_name
    , c.market_segment
    , ct.phone
    , ct.account_balance
    , ct.customer_address
    , coalesce(bv.segment, 'UNKNOWN') as business_segment
    , coalesce(bv.vip_flag, false) as vip_flag
    , coalesce(bv.manager_id, -1) as manager_id
    , {{ record_source('tpch', 'CUSTOMER') }} as record_source
    , current_timestamp() as load_ts
from spine as sp
left join core_asof as c
    on
        sp.h_customer_pk = c.h_customer_pk
        and sp.pit_date = c.pit_date
left join contact_asof as ct
    on
        sp.h_customer_pk = ct.h_customer_pk
        and sp.pit_date = ct.pit_date
left join bv_asof as bv
    on
        sp.h_customer_pk = bv.h_customer_pk
        and sp.pit_date = bv.pit_date

{% if is_incremental() %}
    where
        sp.pit_date > (
            select coalesce(max(t.pit_date), '1900-01-01'::date)
            from {{ this }} as t
        )
{% endif %}
