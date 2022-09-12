
{%- macro delta_apply_macro(target_relation,temp_relation,primary_key, timestamp_now) -%}
    
    {{ create_table_as(false, temp_relation, sql) }}

    {%- set columns = (adapter.get_columns_in_relation(temp_relation)) -%}

    {%- set csv_columns = get_quoted_csv(columns | map(attribute="column")) -%}

    {%- set column_names = csv_columns.split(',') -%}

    {#GIT WORK#}

    merge into {{ target_relation }} 
    using {{ temp_relation }}
    on (
        {% for col in column_names %}
            {{target_relation}}.{{col}} = {{temp_relation}}.{{col}}
            {% if not loop.last %}
                and
            {% endif %}
        {% endfor %}
    )

    when matched then update set 
        {%- for i in range(column_names | length) %}
            {% if i < 5 %}
              {{ target_relation }}.{{column_names[i]}} = {{ temp_relation }}.{{column_names[i]}}{{','}}
            {% endif %}
        {% endfor -%}
        {{ target_relation }}.{{'"EXPIRY_DATE"'}} = cast({{ timestamp_now }} - interval '1 second' as timestamp_ltz)

    
    when not matched then insert 
        values
        (
        {%- for i in range(column_names | length) %} 
            {% if i < 4 %}
                {{ temp_relation }}.{{column_names[i]}}{{','}}
            {% endif %}
        {% endfor -%}

        cast({{ timestamp_now }} as timestamp_ltz),
        cast('12/31/9999 23:59:59' as timestamp_ltz)
        );
        
{%- endmacro -%}