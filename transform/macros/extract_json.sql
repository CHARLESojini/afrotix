{#
  extract_json(column, key)

  Returns a key from a JSON/VARIANT column as text, using the right syntax for
  the active warehouse. Cast the result to the type you need in the model.
  This is the one place the DuckDB-vs-Snowflake dialect difference lives.
#}
{% macro extract_json(column, key) %}
    {%- if target.type == 'duckdb' -%}
        {{ column }}->>'{{ key }}'
    {%- elif target.type == 'snowflake' -%}
        {{ column }}:{{ key }}::string
    {%- else -%}
        {{ exceptions.raise_compiler_error("extract_json not implemented for " ~ target.type) }}
    {%- endif -%}
{% endmacro %}
