{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['sales_key', 'valid_from'],
    tags=['mart', 'fact']
) }}

WITH lineitem_link AS (
    SELECT
        l.l_order_lineitem_pk
        , l.h_order_pk
        , l.h_product_pk
    FROM {{ ref('lnk_order_lineitem') }} AS l
)

, measures_dedup AS (
    SELECT
        s.l_order_lineitem_pk AS sales_key
        , s.extended_price
        , s.quantity
        , s.discount
        , s.load_ts AS valid_from
        , s.hashdiff
    FROM {{ ref('sat_order_lineitem_measures') }} AS s

    {% if is_incremental() %}
        WHERE s.load_ts >= DATEADD(
            DAY, 0
            , (
                SELECT COALESCE(MAX(t.valid_from), TO_TIMESTAMP_TZ('1900-01-01'))
                FROM {{ this }} AS t
            )
        )
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.l_order_lineitem_pk, s.load_ts
        ORDER BY s.hashdiff DESC
    ) = 1
)

, order_sat_hist AS (
    SELECT
        s.h_order_pk
        , s.order_date::DATE AS order_date
        , s.load_ts
    FROM {{ ref('sat_order_core') }} AS s
)

, order_customer_hist AS (
    SELECT
        l.h_order_pk
        , l.h_customer_pk
        , l.load_ts
    FROM {{ ref('lnk_order_customer') }} AS l
)

, base_pairs AS (
    SELECT
        m.sales_key
        , m.valid_from
        , ll.h_order_pk
        , ll.h_product_pk
        , m.extended_price
        , m.quantity
        , m.discount
    FROM measures_dedup AS m
    INNER JOIN lineitem_link AS ll
        ON m.sales_key = ll.l_order_lineitem_pk
)

, order_asof AS (
    SELECT
        b.sales_key
        , b.valid_from
        , os.order_date
        , ROW_NUMBER() OVER (
            PARTITION BY b.sales_key, b.valid_from
            ORDER BY os.load_ts DESC
        ) AS rn
    FROM base_pairs AS b
    INNER JOIN order_sat_hist AS os
        ON
            b.h_order_pk = os.h_order_pk
            AND b.valid_from >= os.load_ts
)

, order_asof_pick AS (
    SELECT
        oa.sales_key
        , oa.valid_from
        , oa.order_date
    FROM order_asof AS oa
    WHERE oa.rn = 1
)

, customer_asof AS (
    SELECT
        b.sales_key
        , b.valid_from
        , oc.h_customer_pk
        , ROW_NUMBER() OVER (
            PARTITION BY b.sales_key, b.valid_from
            ORDER BY oc.load_ts DESC
        ) AS rn
    FROM base_pairs AS b
    LEFT JOIN order_customer_hist AS oc
        ON
            b.h_order_pk = oc.h_order_pk
            AND b.valid_from >= oc.load_ts
)

, customer_asof_pick AS (
    SELECT
        ca.sales_key
        , ca.valid_from
        , ca.h_customer_pk
    FROM customer_asof AS ca
    WHERE ca.rn = 1
)

, fact_versions_raw AS (
    SELECT
        b.sales_key
        , c.h_customer_pk
        , b.h_product_pk
        , o.order_date
        , b.extended_price
        , b.quantity
        , b.discount
        , b.valid_from
    FROM base_pairs AS b
    INNER JOIN order_asof_pick AS o
        ON
            b.sales_key = o.sales_key
            AND b.valid_from = o.valid_from
    LEFT JOIN customer_asof_pick AS c
        ON
            b.sales_key = c.sales_key
            AND b.valid_from = c.valid_from
)

, fact_versions AS (
    SELECT
        f.*
        , LEAD(f.valid_from) OVER (
            PARTITION BY f.sales_key
            ORDER BY f.valid_from
        ) AS next_valid_from
    FROM fact_versions_raw AS f
)

, new_rows AS (
    SELECT
        fv.sales_key
        , fv.h_customer_pk
        , fv.h_product_pk
        , d.date_key AS order_date_key
        , dc.customer_key
        , dp.product_key
        , fv.extended_price
        , fv.quantity
        , fv.discount
        , fv.extended_price * (1 - fv.discount) AS net_amount
        , fv.valid_from
        , COALESCE(fv.next_valid_from, TO_TIMESTAMP_TZ('9999-12-31')) AS valid_to
        , (fv.next_valid_from IS NULL) AS is_current
        , CAST('{{ run_started_at }}' AS TIMESTAMP_TZ) AS load_ts
    FROM fact_versions AS fv
    LEFT JOIN {{ ref('dim_date') }} AS d
        ON fv.order_date = d.date_key
    LEFT JOIN {{ ref('dim_customer') }} AS dc
        ON
            fv.h_customer_pk = dc.h_customer_pk
            AND (fv.valid_from)::DATE >= dc.valid_from
            AND (fv.valid_from)::DATE < dc.valid_to
    LEFT JOIN {{ ref('dim_product') }} AS dp
        ON fv.h_product_pk = dp.h_product_pk
)

{% if is_incremental() %}
    , to_close AS (
        SELECT
            t.sales_key
            , t.valid_from
            , MIN(n.valid_from) AS new_valid_from
        FROM {{ this }} AS t
        INNER JOIN new_rows AS n
            ON t.sales_key = n.sales_key
        WHERE
            t.is_current = TRUE
            AND n.valid_from > t.valid_from
        GROUP BY
            t.sales_key
            , t.valid_from
    )

    , closing_rows AS (
        SELECT
            t.sales_key
            , t.h_customer_pk
            , t.h_product_pk
            , t.order_date_key
            , t.customer_key
            , t.product_key
            , t.extended_price
            , t.quantity
            , t.discount
            , t.net_amount
            , t.valid_from
            , c.new_valid_from AS valid_to
            , FALSE AS is_current
            , CAST('{{ run_started_at }}' AS TIMESTAMP_TZ) AS load_ts
        FROM {{ this }} AS t
        INNER JOIN to_close AS c
            ON
                t.sales_key = c.sales_key
                AND t.valid_from = c.valid_from
    )
{% endif %}

{% if is_incremental() %}
    SELECT * FROM closing_rows
    UNION ALL
    SELECT * FROM new_rows
{% else %}
SELECT * FROM new_rows
{% endif %}
