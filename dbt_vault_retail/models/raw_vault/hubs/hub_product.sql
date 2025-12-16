{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['h_product_pk'],
    tags=['raw_vault', 'hub']
) }}

WITH src AS (
    SELECT DISTINCT
        l_partkey AS bk_part_id
        , {{ record_source('tpch', 'LINEITEM') }} AS record_source
        , sha2(coalesce(to_varchar(l_partkey), ''), 256) AS h_product_pk
        , current_timestamp() AS load_ts
        , current_date() AS load_date
    FROM {{ source('tpch_sf1', 'LINEITEM') }}
)

SELECT
    s.bk_part_id
    , s.h_product_pk
    , s.record_source
    , s.load_ts
    , s.load_date
FROM src AS s

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE t.h_product_pk = s.h_product_pk
    )
{% endif %}
