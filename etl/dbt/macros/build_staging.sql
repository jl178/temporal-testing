{#-
  Schema-driven staging: the canonical model (this model's columns in
  schema.yml) IS the transformation. For each declared column, emit:
    - meta.expr verbatim when set   (cleanup semantics: lower(trim(x)), …)
    - cast(name as data_type)       when a non-string data_type is declared
    - the bare column               otherwise
  Adding a field to the canonical model = one schema.yml block; the SQL,
  the landing gate, and the tests all follow from it.
-#}
{% macro build_staging(source_name, table_name) %}
select
{%- for name, col in model.columns.items() %}
    {%- set expr = col.meta.expr if col.meta and col.meta.expr
                   else ("cast(" ~ name ~ " as " ~ col.data_type ~ ")"
                         if col.data_type and col.data_type != "string"
                         else name) %}
    {{ expr }} as {{ name }}{{ "," if not loop.last }}
{%- endfor %}
from {{ source(source_name, table_name) }}
{% endmacro %}
