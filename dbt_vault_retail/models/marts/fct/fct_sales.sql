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

, measures AS (
    SELECT
        s.l_order_lineitem_pk
        , s.extended_price
        , s.quantity
        , s.discount
        , s.load_ts
        , s.hashdiff
    FROM {{ ref('sat_order_lineitem_measures') }} AS s

    {% if is_incremental() %}
        WHERE s.load_ts >= dateadd(
            DAY, 0
            , (
                SELECT coalesce(max(t.valid_from), to_timestamp_tz('1900-01-01'))
                FROM {{ this }} AS t
            )
        )
    {% endif %}
)

, order_sat AS (
    SELECT
        s.h_order_pk
        , s.order_date::date AS order_date
    FROM {{ ref('sat_order_core') }} AS s
    QUALIFY row_number() OVER (
        PARTITION BY s.h_order_pk
        ORDER BY s.load_ts DESC, s.hashdiff DESC
    ) = 1
)

, order_customer AS (
    SELECT
        l.h_order_pk
        , l.h_customer_pk
    FROM {{ ref('lnk_order_customer') }} AS l
    QUALIFY row_number() OVER (
        PARTITION BY l.h_order_pk
        ORDER BY l.load_ts DESC
    ) = 1
)

, fact_versions_raw AS (
    SELECT
        m.l_order_lineitem_pk AS sales_key
        , oc.h_customer_pk
        , ll.h_product_pk
        , os.order_date
        , m.extended_price
        , m.quantity
        , m.discount
        , m.load_ts AS valid_from
    FROM measures AS m
    INNER JOIN lineitem_link AS ll
        ON m.l_order_lineitem_pk = ll.l_order_lineitem_pk
    INNER JOIN order_sat AS os
        ON ll.h_order_pk = os.h_order_pk
    LEFT JOIN order_customer AS oc
        ON ll.h_order_pk = oc.h_order_pk
)

, fact_versions AS (
    SELECT
        f.*
        , lead(f.valid_from) OVER (
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
        , coalesce(fv.next_valid_from, to_timestamp_tz('9999-12-31')) AS valid_to
        , (fv.next_valid_from IS NULL) AS is_current

        , cast('{{ run_started_at }}' AS timestamp_tz) AS load_ts
    FROM fact_versions AS fv
    LEFT JOIN {{ ref('dim_date') }} AS d
        ON fv.order_date = d.date_key
    LEFT JOIN {{ ref('dim_customer') }} AS dc
        ON
            fv.h_customer_pk = dc.h_customer_pk
            AND (fv.valid_from)::date >= dc.valid_from
            AND (fv.valid_from)::date < dc.valid_to
    LEFT JOIN {{ ref('dim_product') }} AS dp
        ON fv.h_product_pk = dp.h_product_pk
)

{% if is_incremental() %}
    , to_close AS (
        SELECT
            t.sales_key
            , t.valid_from
            , min(n.valid_from) AS new_valid_from
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
            , cast('{{ run_started_at }}' AS timestamp_tz) AS load_ts
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
select * from new_rows
{% endif %}
