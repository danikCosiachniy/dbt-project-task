with source as (
    select * from {{ ref('raw_orders') }}
),

hashed as (
    select
        md5(cast(upper(trim(cast(order_id as varchar))) as varchar)) as hk_order_id,
        md5(cast(upper(trim(cast(customer_id as varchar))) as varchar)) as hk_customer_id,
        md5(concat(cast(status as varchar), cast(amount as varchar))) as hash_diff,
        
        order_id,
        customer_id,
        status,
        amount,
        order_date,
        current_timestamp as load_date,
        'LOCAL_CSV' as record_source
    from source
)

select * from hashed