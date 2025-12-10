{% macro hash_sha256(expr) -%}
  {{ adapter.dispatch('hash_sha256', 'retail_vault')(expr) }}
{%- endmacro %}

{# default (Snowflake) #}
{% macro default__hash_sha256(expr) -%}
  sha2(cast({{ expr }} as varchar), 256)
{%- endmacro %}

{# DuckDB override #}
{% macro duckdb__hash_sha256(expr) -%}
  sha256(cast({{ expr }} as varchar))
{%- endmacro %}
