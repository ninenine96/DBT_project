{% macro get_column_names(table_name = target.table, prefix = '') %}
  
  {% set query_table %}
    use database {{ target.database }};
    select * from {{ table_name }}
  {% endset %}
  {% set result_names = [] %}

  {% if execute %}
    {% set col_names = run_query(query_table).columns.keys() %}
      {% if prefix %}
        {% for name in col_names %}
            {{ result_names.append(prefix~name) }} 
        {% endfor %}
      {% else %}
        {% for name in col_names %}
            {{ result_names.append(name) }}
        {% endfor %} 
      {% endif %}
      
        {{ return(result_names) }}
    {% else %}
      {{ return }}
  {% endif %}

{% endmacro %}
