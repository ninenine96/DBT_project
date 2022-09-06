{% materialization m_type, default %}
    {%- set target_relation = this %}
    {% set new_name = config.get('prefix') ~ this.table %}
    {{print("sql --> " ~ sql)}}

    {% call statement('main') -%}
        create or replace table {{new_name}} as {{sql}}
    {%- endcall %}
    {% set target_relation = this.incorporate(type='table') %}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}