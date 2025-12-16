{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['l_order_lineitem_pk', 'load_ts', 'record_source'],
    tags=['raw_vault', 'sat']
) }}

WITH src AS (
    SELECT
        sha2(
            coalesce(to_varchar(l_orderkey), '') || '|'
            || coalesce(to_varchar(l_linenumber), '') || '|'
            || coalesce(to_varchar(l_partkey), '')
            , 256
        ) AS l_order_lineitem_pk

        , l_quantity AS quantity
        , l_extendedprice AS extended_price
        , l_discount AS discount
        , l_tax AS tax
        , l_shipdate AS ship_date
        , l_commitdate AS commit_date
        , l_receiptdate AS receipt_date

        , sha2(
            coalesce(to_varchar(l_quantity), '') || '|'
            || coalesce(to_varchar(l_extendedprice), '') || '|'
            || coalesce(to_varchar(l_discount), '') || '|'
            || coalesce(to_varchar(l_tax), '') || '|'
            || coalesce(to_varchar(l_shipdate), '') || '|'
            || coalesce(to_varchar(l_commitdate), '') || '|'
            || coalesce(to_varchar(l_receiptdate), '')
            , 256
        ) AS hashdiff

        , {{ record_source('tpch', 'LINEITEM') }} AS record_source
        , current_timestamp() AS load_ts
        , current_date() AS load_date
    FROM {{ source('tpch_sf1', 'LINEITEM') }}
)

{% if is_incremental() %}
, latest as (
    select
        l_order_lineitem_pk
        , record_source
        , hashdiff
    from {{ this }}
    qualify row_number() over (
        partition by l_order_lineitem_pk, record_source
        order by load_ts desc
    ) = 1
)
{% endif %}

SELECT
    s.l_order_lineitem_pk
    , s.quantity
    , s.extended_price
    , s.discount
    , s.tax
    , s.ship_date
    , s.commit_date
    , s.receipt_date
    , s.hashdiff
    , s.record_source
    , s.load_ts
    , s.load_date
FROM src AS s

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest AS l
        WHERE s.l_order_lineitem_pk = l.l_order_lineitem_pk
          AND s.record_source = l.record_source
          AND s.hashdiff = l.hashdiff
    )
{% endif %}
