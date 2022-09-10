{%- materialization full_apply, adapter='snowflake' %}
    {% set identifier= config.get('target_table')%}
    {% set target_rel= this.database~'.'~this.schema~'.'~identifier%}
    {%- set target_relation = adapter.get_relation(database=this.database,schema=this.schema,identifier=identifier) -%}
    {%- set target_table = target_relation.identifier %}
    {%- set temp_table= this -%}
    {%- set primary_key = config.get('primary_key') %}
    {% if target_relation is not none %}    
        {%- set build_sql = full_apply_macro(target_table, temp_table,primary_key) %}
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

{%- macro full_apply_macro(target_relation, temp_relation,primary_key) -%}
    {{ create_table_as(false, temp_relation, sql) }}
    {%- set columns = (adapter.get_columns_in_relation(temp_relation)) -%}
    {%- set csv_columns = get_quoted_csv(columns | map(attribute="name")) -%}
    {%- set column_names=csv_columns.split(',')-%}
    {{print(csv_columns)}}
    INSERT INTO {{ target_relation }}
    select *,'inserted' 
    from {{temp_relation}}  
    where not exists(
        SELECT {{primary_key}} 
        FROM {{target_relation}} 
        WHERE {{target_relation}}.{{primary_key}}={{temp_relation}}.{{primary_key}}
    );
    UPDATE {{target_relation}} 
    SET STATUS='DELETED' 
    WHERE NOT EXISTS(
        SELECT {{primary_key}} 
        FROM {{temp_relation}}
        WHERE {{temp_relation}}.{{primary_key}}={{target_relation}}.{{primary_key}}
    );
    {% set temp_sql %}
      ( select temp.*,hash(target.*)=hash(temp.*) as hash from {{temp_relation}} temp
        left join
        ( select {{csv_columns}} FROM {{target_relation}} ) target
        ON temp.{{primary_key}}= target.{{primary_key}}
        where hash=false )
    {% endset %}
    {% set tmp_table = '"DBTD_PROJECT"."DBTS_PROJECT"."TMP_TABLE"' %}
    {{ create_table_as(TRUE,tmp_table, temp_sql) }}
    merge into {{target_relation}} 
    using {{tmp_table}}
    on ({{target_relation}}.{{primary_key}} = {{tmp_table}}.{{primary_key}}) 
    when matched then update set
        {% for i in column_names %}
        {{target_relation}}.{{i}}= {{tmp_table}}.{{i}},
        {% endfor %}
        {{target_relation}}.status='updated'
    ;
{%- endmacro -%}

{%- macro create_table(target_rel,temp_relation) -%}
    {%- set initial_sql -%}
        SELECT
          * 
        FROM
          {{ temp_relation }}
    {%- endset -%}
    {{ create_table_as(False, target_rel, initial_sql) }}
    alter table {{target_rel}}
    add status nvarchar;
    update {{target_rel}}
    set status ='inserted'
{%- endmacro -%}

