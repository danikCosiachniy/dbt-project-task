{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['raw_vault', 'hub']
) }}

WITH src AS (
    SELECT DISTINCT
        customer_id AS bk_customer_id
        , h_customer_pk
        , record_source
        , load_ts
    FROM {{ ref('stg_customer') }}
)

SELECT *
FROM src AS s

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE t.h_customer_pk = s.h_customer_pk
    )
{% endif %}
