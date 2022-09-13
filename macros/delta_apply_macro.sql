
{%- macro delta_apply_macro(target_relation,temp_relation,primary_key, timestamp_now) -%}

    {%- set columns = (adapter.get_columns_in_relation(temp_relation)) -%}

    {%- set csv_columns = get_quoted_csv(columns | map(attribute="column")) -%}

    {%- set column_names = csv_columns.split(',') -%}

    INSERT INTO {{ target_relation }}
    select *
    from {{ temp_relation }}  
    where not exists(
        SELECT {{ primary_key }} 
        FROM {{ target_relation }} 
        WHERE 
        {%- for i in range(column_names | length - 1) %}
              {{ target_relation }}.{{column_names[i]}} = {{ temp_relation }}.{{ column_names[i] }}
              {% if not loop.last %}
                AND 
              {% endif %}
        {% endfor %}
    );

    merge into {{ target_relation }} 
    using {{ temp_relation }}
    on (
        {%- for i in range(column_names | length - 3) %}
              {{ target_relation }}.{{column_names[i]}} = {{ temp_relation }}.{{ column_names[i] }}
              {% if not loop.last %}
                AND 
              {% endif %}
        {% endfor %}
    )

    when matched and {{ target_relation }}.RECORD_DATE < {{ temp_relation }}.RECORD_DATE
        then update set 
        {{ target_relation }}.{{'EXPIRY_DATE'}} = cast({{ timestamp_now }} + interval '-1 second' as datetime)

    
    when not matched
        then insert values 
        (
        {%- for i in range(column_names | length) %} 
            {% if i < 4 %}
                {{ temp_relation }}.{{column_names[i]}}{{','}}
            {% endif %}
        {% endfor -%}

        cast({{ timestamp_now }} as datetime),
        cast('12/31/9999 23:59:59' as datetime)
        );
        
{%- endmacro -%}