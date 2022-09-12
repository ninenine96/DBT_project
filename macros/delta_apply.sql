{%- materialization delta_apply, default %}
    {% set identifier= config.get('target_table')%}
    {% set timestamp_now = current_timestamp() %} 
    {% set target_rel= this.database~'.'~this.schema~'.'~identifier%}
    {%- set target_relation = adapter.get_relation(database=this.database,schema=this.schema,identifier=identifier) -%}
    {%- set target_table = target_relation.identifier %}
    {%- set temp_table= this -%}
    {%- set primary_key = config.get('primary_key') %}

    {% if target_relation is not none %}    
        {%- set build_sql = delta_apply_macro(target_table,temp_table,primary_key, timestamp_now) %}
    {% else %}
        {%- set build_sql = create_table(target_rel,temp_table) %}
    {% endif %}

    {%- call statement('main') -%}
        {{ build_sql }}
    {% endcall %}
    
    {% set target_relation = this.incorporate(type='table') %}
    {% do persist_docs(target_relation, model) %}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization -%}
