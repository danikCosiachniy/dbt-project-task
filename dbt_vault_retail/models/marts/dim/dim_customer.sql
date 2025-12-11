{{ config(materialized='table', tags=['mart', 'dimension']) }}

with pit as (
    select
        h_customer_pk
        , pit_date
        , customer_name
        , market_segment
        , phone
        , account_balance
        , customer_address
        , business_segment
        , vip_flag
        , manager_id
    from {{ ref('pit_customer') }}
)

, ranges as (
    select
        h_customer_pk
        , pit_date as valid_from
        , customer_name
        , market_segment
        , phone
        , account_balance
        , customer_address
        , business_segment
        , vip_flag
        , manager_id
        , lead(pit_date) over (
            partition by h_customer_pk
            order by pit_date
        ) as next_date
    from pit
)

, final as (
    select
        h_customer_pk
        , valid_from
        , customer_name
        , market_segment
        , phone

        , account_balance
        , customer_address
        , business_segment
        , vip_flag
        , manager_id
        , sha2(
            coalesce(to_varchar(h_customer_pk), '') || '|'
            || coalesce(to_varchar(valid_from), '')
            , 256
        ) as customer_key
        , coalesce(next_date - interval '1 day', '9999-12-31'::date) as valid_to
        , coalesce(next_date is null, false) as is_current
    from ranges
)

select * from final
