{{ config(materialized='table', tags=['mart', 'fact']) }}

WITH lineitem_sat AS (

    SELECT
        l_order_lineitem_pk
        , extended_price
        , quantity
        , discount
    FROM {{ ref('sat_order_lineitem_measures') }}
    QUALIFY row_number() OVER (
        PARTITION BY l_order_lineitem_pk
        ORDER BY load_ts DESC
    ) = 1

)

, lineitem_link AS (

    SELECT
        l_order_lineitem_pk
        , h_order_pk
        , h_product_pk
    FROM {{ ref('lnk_order_lineitem') }}

)

, hub_product AS (

    SELECT
        h_product_pk
        , bk_part_id AS part_id
    FROM {{ ref('hub_product') }}

)

, order_customer AS (

    SELECT
        h_order_pk
        , h_customer_pk
        , pit_date
    FROM {{ ref('pit_order_customer') }}

)

, order_pit AS (

    SELECT
        h_order_pk
        , pit_date
    FROM {{ ref('pit_order') }}

)

, base AS (

    SELECT
        l.l_order_lineitem_pk
        , oc.h_customer_pk
        , l.h_product_pk
        , hp.part_id
        , op.pit_date AS order_date
        , s.extended_price
        , s.quantity
        , s.discount
    FROM lineitem_link AS l
    INNER JOIN lineitem_sat AS s
        ON l.l_order_lineitem_pk = s.l_order_lineitem_pk
    INNER JOIN order_pit AS op
        ON l.h_order_pk = op.h_order_pk
    LEFT JOIN order_customer AS oc
        ON
            l.h_order_pk = oc.h_order_pk
            AND op.pit_date = oc.pit_date
    LEFT JOIN hub_product AS hp
        ON l.h_product_pk = hp.h_product_pk

)

, dim_cust AS (
    SELECT * FROM {{ ref('dim_customer') }}
)

, dim_prod AS (
    SELECT * FROM {{ ref('dim_product') }}
)

, dim_d AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    b.l_order_lineitem_pk AS sales_key
    , d.date_key AS order_date_key

    , dc.customer_key
    , dp.product_key
    , b.extended_price

    , b.quantity
    , b.discount
    , b.extended_price * (1 - b.discount) AS net_amount
FROM base AS b
LEFT JOIN dim_d AS d
    ON b.order_date = d.date_key
LEFT JOIN dim_cust AS dc
    ON
        b.h_customer_pk = dc.h_customer_pk
        AND b.order_date BETWEEN dc.valid_from AND dc.valid_to
LEFT JOIN dim_prod AS dp
    ON b.part_id = dp.part_id
