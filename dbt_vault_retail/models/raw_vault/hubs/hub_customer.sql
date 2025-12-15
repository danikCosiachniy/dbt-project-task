{{ config(
    materialized='incremental',
    unique_key='h_customer_pk',
    incremental_strategy='merge',
    tags=['raw_vault', 'hub']
) }}

WITH src AS (
    SELECT DISTINCT
        customer_id AS bk_customer_id
        , {{ record_source('tpch', 'CUSTOMER') }} AS record_source
        , sha2(coalesce(to_varchar(customer_id), ''), 256) AS h_customer_pk
        , current_timestamp() AS load_ts
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
