{%- materialization full_apply, adapter='snowflake'  -%}
    {%- set e_time = current_timestamp()-%}
    {%- set target_database= config.get('target_database') -%}
    {%- set target_schema= config.get('target_schema') -%}
    {%- set target_table= config.get('target_table') -%}
    {%- set temp_table= this.table -%}
    {%- set target_relation = adapter.get_relation(database=target_database,schema=target_schema,identifier=target_table) -%}
    {%- set target_rel=target_database~'.'~target_schema~'.'~target_table -%}

    {%- if target_relation is not none  -%}  
        {%- set build_sql =full_apply_macro(target_table, temp_table,e_time) -%}
    {%- else %}
        {%- set build_sql = create_table(target_rel,temp_table,e_time)  -%}
    {%- endif  -%}

    {%- call statement('main') -%}
        {{ build_sql }}
    {%- endcall %}
    {% set target_relation = this.incorporate(type='table') %}
    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}

{%- macro full_apply_macro(target_relation, temp_relation, e_time) -%}
    {{ create_table_as(true, temp_relation, sql) }}
    {%- set columns = (adapter.get_columns_in_relation(temp_relation)) -%}
    {%- set column_names_csv = get_quoted_csv(columns | map(attribute="name")) -%}
    {%- set column_names_list =column_names_csv.split(',')-%}
    {%- set primary_keys_csv = config.get('primary_key')  -%}
    {%- set primary_keys_list = (primary_keys_csv.split(',')) -%}
    INSERT INTO {{ target_relation }}
    select *,cast({{e_time}} as timestamp_ntz),NULL
    from {{temp_relation}}  
    where not exists(
        SELECT {{primary_keys_csv}} 
        FROM {{target_relation}} 
        WHERE 
        {% for i in primary_keys_list %}
                {{target_relation}}.{{i}} = {{temp_relation}}.{{i}}
                {% if not loop.last %}
                    and
                {% endif %}
        {% endfor %}   
    );
    UPDATE {{target_relation}} 
    SET  ExpiryDate=cast({{e_time}} as timestamp_ntz)+ interval '-1 second'
    WHERE NOT EXISTS(
        SELECT {{primary_keys_csv}} 
        FROM {{temp_relation}}
        WHERE 
        {% for key in primary_keys_list %}
                {{temp_relation}}.{{key}}={{target_relation}}.{{key}}
                {% if not loop.last %}
                    and
                {% endif %}
        {% endfor %}
        
    );
    {%- set temp_sql -%}
      ( select temp.*  from {{temp_relation}} temp
        inner join {{target_relation}} target
        on
        {% for key in primary_keys_list %}
                temp.{{key}}= target.{{key}}
                {% if not loop.last %}
                    and
                {% endif %}
        {% endfor %}
        where (
        {% for key in column_names_list %}
                temp.{{key}}!= target.{{key}}
                {% if not loop.last %}
                    or
                {% endif %}
        {% endfor %}
        )and target.ExpiryDate is null)
        
    {%- endset -%}
    {%- set tmp_table =this.database~'.'~this.schema~'.'~'TMP_TABLE' -%}
    {{ create_table_as(TRUE,tmp_table, temp_sql) }}
    merge into {{target_relation}} 
    using {{tmp_table}}
    on  
    {% for key in primary_keys_list %}
    {{target_relation}}.{{key}} = {{tmp_table}}.{{key}} and
                
    {% endfor %}
                {{target_relation}}.ExpiryDate is null
    
    when matched then update set
        ExpiryDate=cast({{e_time}} as timestamp_ntz)+ interval '-1 second';
    insert into {{target_relation}}
    select {{column_names_csv}}, cast({{e_time}} as timestamp_ntz),null from {{tmp_table}};

{%- endmacro -%}

{%- macro create_table(target_rel,temp_relation,e_time) -%}
    {{ create_table_as(true, temp_relation, sql) }}
    {%- set initial_sql -%}
        SELECT
          * 
        FROM
          {{ temp_relation }}
    {%- endset -%}
    {{ create_table_as(False, target_rel, initial_sql) }}
    alter table {{target_rel}}
    add EffectiveDate timestamp_tz(9),
    ExpiryDate timestamp_tz(9);
    update {{target_rel}}
    set EffectiveDate =cast({{e_time}} as timestamp_ntz);
{%- endmacro -%}