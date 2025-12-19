{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['raw_vault', 'hub']
) }}

WITH src AS (
    SELECT
        part_id AS bk_part_id
        , h_product_pk
        , record_source
        , load_ts
    FROM {{ ref('stg_lineitem') }}
)

SELECT *
FROM src

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE t.h_product_pk = s.h_product_pk
    )
{% endif %}
