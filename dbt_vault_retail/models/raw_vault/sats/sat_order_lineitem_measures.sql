{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['raw_vault', 'sat']
) }}

WITH src AS (
    SELECT
        l_order_lineitem_pk
        , quantity
        , extended_price
        , discount
        , tax
        , ship_date
        , commit_date
        , receipt_date
        , hashdiff
        , record_source
        , load_ts
    FROM {{ ref('stg_lineitem') }}

    {% if is_incremental() %}
    where ship_date >= dateadd(
        day, 0,
        (select coalesce(max(t.ship_date), to_date('1992-01-02')) from {{ this }} as t)
    )
    {% endif %}
)

{% if is_incremental() %}
, latest as (
    select
        l_order_lineitem_pk,
        record_source,
        hashdiff
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
