-- Sobreescribe el comportamiento default de dbt que genera nombres como
-- "<target_schema>_<custom_schema>". Con este macro, si se define un schema
-- personalizado en el modelo (via +schema o config(schema=...)), se usa
-- directamente ese nombre sin prefijo. Así staging queda en "staging",
-- marts en "marts", etc., respetando los schemas creados en dw_init.sql.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
