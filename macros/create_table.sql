
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

