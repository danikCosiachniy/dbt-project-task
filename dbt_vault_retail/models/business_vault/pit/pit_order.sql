{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['business_vault', 'pit']
) }}

WITH as_of AS (
    SELECT DISTINCT
        h_order_pk
        , record_source
        , effective_from AS as_of_ts
    FROM {{ ref('eff_sat_order_status') }}

    {% if is_incremental() %}
        WHERE effective_from > (
            SELECT coalesce(max(t.as_of_ts), to_timestamp_tz('1900-01-01'))
            FROM {{ this }} AS t
        )
    {% endif %}
)

, pit AS (
    SELECT
        a.h_order_pk
        , a.record_source
        , a.as_of_ts
        , e.effective_from AS eff_load_ts
        , row_number() OVER (
            PARTITION BY a.h_order_pk, a.record_source, a.as_of_ts
            ORDER BY e.effective_from DESC
        ) AS rn
    FROM as_of AS a
    LEFT JOIN {{ ref('eff_sat_order_status') }} AS e
        ON
            a.h_order_pk = e.h_order_pk
            AND a.record_source = e.record_source
            AND a.as_of_ts >= e.effective_from
            AND a.as_of_ts < e.effective_to
    QUALIFY rn = 1
)

SELECT
    p.h_order_pk
    , p.record_source
    , p.as_of_ts
    , p.eff_load_ts
    , cast('{{ run_started_at }}' AS timestamp_tz) AS load_ts
FROM pit AS p

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE
            t.h_order_pk = p.h_order_pk
            AND t.record_source = p.record_source
            AND t.as_of_ts = p.as_of_ts
    )
{% endif %}
