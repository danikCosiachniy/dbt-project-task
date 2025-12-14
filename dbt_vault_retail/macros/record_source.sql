{% macro record_source(base_key, suffix=None) -%}
  {%- set base = var('dv_record_source_base')[base_key] -%}
  '{{ base }}{% if suffix %}:{{ suffix }}{% endif %}'
{%- endmacro %}
