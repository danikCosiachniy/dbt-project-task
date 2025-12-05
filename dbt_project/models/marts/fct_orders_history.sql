{{ config(materialized='table') }}

WITH orders_hub AS (
    SELECT * FROM {{ ref('hub_order') }}
)

, orders_sat AS (
    SELECT * FROM {{ ref('sat_order_details') }}
)

, final AS (
    SELECT
        h.order_id
        , s.order_date
        , s.status
        , s.load_date AS dv_load_date
    FROM orders_hub AS h
    INNER JOIN orders_sat AS s
        ON h.order_hk = s.order_hk
)

SELECT * FROM final
