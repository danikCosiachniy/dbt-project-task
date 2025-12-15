{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_order_lineitem_pk'],
    tags=['raw_vault', 'link']
) }}

WITH src AS (
    SELECT
        l_orderkey AS order_id
        , l_linenumber AS line_number
        , l_partkey AS part_id
        , {{ record_source('tpch', 'LINEITEM') }} AS record_source
        , sha2(coalesce(to_varchar(l_orderkey), ''), 256) AS h_order_pk
        , sha2(coalesce(to_varchar(l_partkey), ''), 256) AS h_product_pk
        , sha2(
            coalesce(to_varchar(l_orderkey), '')
            || '|'
            || coalesce(to_varchar(l_linenumber), '')
            || '|'
            || coalesce(to_varchar(l_partkey), '')
            , 256
        ) AS l_order_lineitem_pk
        , current_timestamp() AS load_ts
        , current_date() AS load_date
    FROM {{ source('tpch_sf1', 'LINEITEM') }}
)

SELECT
    s.l_order_lineitem_pk
    , s.h_order_pk
    , s.h_product_pk
    , s.record_source
    , s.load_ts
    , s.load_date
FROM src AS s

{% if is_incremental() %}
    left join {{ this }} as t
        on s.l_order_lineitem_pk = t.l_order_lineitem_pk
    where t.l_order_lineitem_pk is null
{% endif %}
