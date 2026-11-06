{#- Use the custom schema as the whole dataset name (g07_silver, g07_gold)
    instead of dbt's default <target dataset>_<custom schema>. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.dataset }}
    {%- else -%}
        g07_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
