{{ config(materialized='table', tags=['mart', 'dimension']) }}

WITH hub_product AS (
    SELECT
        h_product_pk
        , bk_part_id AS part_id
    FROM {{ ref('hub_product') }}
)

, lnk AS (
    SELECT
        l_order_lineitem_pk
        , h_product_pk
    FROM {{ ref('lnk_order_lineitem') }}
)

, sat AS (
    SELECT
        l_order_lineitem_pk
        , extended_price
        , load_ts
        , hashdiff
    FROM {{ ref('sat_order_lineitem_measures') }}
    QUALIFY row_number() OVER (
        PARTITION BY l_order_lineitem_pk
        ORDER BY load_ts DESC
    ) = 1
)

, parts AS (
    SELECT
        hp.part_id
        , hp.h_product_pk
        , min(s.extended_price) AS min_price
        , max(s.extended_price) AS max_price
        , avg(s.extended_price) AS avg_price
    FROM sat AS s
    INNER JOIN lnk AS l
        ON s.l_order_lineitem_pk = l.l_order_lineitem_pk
    INNER JOIN hub_product AS hp
        ON l.h_product_pk = hp.h_product_pk
    GROUP BY
        hp.part_id
        , hp.h_product_pk
)

SELECT
    part_id
    , h_product_pk
    , min_price
    , max_price
    , avg_price
    , sha2(coalesce(to_varchar(h_product_pk), ''), 256) AS product_key
FROM parts
