
{%- macro delta_apply_macro(target_relation,temp_relation,primary_key) -%}
    
    {{ create_table_as(false, temp_relation, sql) }}
    
    {%- set columns = (adapter.get_columns_in_relation(temp_relation)) -%}
    {%- set csv_columns = get_quoted_csv(columns | map(attribute="name")) -%}
    {%- set column_names=csv_columns.split(',')-%}

    {% set temp_sql %}
      ( select temp.*,
            hash(
            {% for col in column_names[:-2] %}
                {{'target'}}{{'.'}}{{ col }}
                {% if not loop.last %}
                  {{','}}
                {% endif %}
            {% endfor %}
            )
            =
            hash(
            {% for col in column_names[:-2] %}
                {{'temp'}}{{'.'}}{{ col }}
                {% if not loop.last %}
                  {{','}}
                {% endif %}
            {% endfor %}
            )
            as hash 
        from {{temp_relation}} temp
        left join
        ( select {{ csv_columns }} FROM {{ target_relation }} ) target
        ON temp.{{primary_key}}= target.{{primary_key}} 
        where hash = false
        )

    {% endset %}

    {% set tmp_table = this.database~'.'~this.schema~'.'~'"TMP_TABLE"' %}

    {{ create_table_as(TRUE,tmp_table, temp_sql) }}

    
    merge into {{ target_relation }} 
    using {{ tmp_table }}
    on ({{ target_relation }}.{{ primary_key }} = {{ tmp_table }}.{{ primary_key }}) 
    when matched then update set
        {%- for i in range(column_names | length) %}
            {% if i < 4 %}
              {{ target_relation }}.{{column_names[i]}} = {{ tmp_table }}.{{column_names[i]}}{{','}}
            {% endif %}
        {% endfor %}
        {{ target_relation }}.{{'"EXPIRY_DATE"'}} = cast(current_timestamp - interval '1 second' as timestamp_ntz)
    when not matched then insert 
        values
        (
        {%- for i in range(column_names | length) %}
            {% if i < 4 %}
            {{ tmp_table }}.{{column_names[i]}}{{','}}
            {% endif %}
        {% endfor -%}
        cast(current_timestamp as timestamp_ntz){{','}}
        cast('12/31/9999 23:59:59' as timestamp_ntz)
        );
{%- endmacro -%}


