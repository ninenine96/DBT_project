with old_employee as (
    select * from {{ ref('stg_delta_employee_all') }}
),

new_employee as (
    select * from {{ ref('stg_delta_employee_new') }}
),

result as (
{% set cols_old = dbt_utils.star(from=ref('stg_delta_employee_all')) %}
{% set cols_new = dbt_utils.star(from=ref('stg_delta_employee_new')) %}
{%- for col in cols_old %}
    {% set col_old = dbt_utils.get_column_values(table=ref('stg_delta_employee_all'), column=col) %}
    {% set col_new = dbt_utils.get_column_values(table=ref('stg_delta_employee_new'), column=col) %}
    
    {% for value in col_old %}
        {% if value in col_new %}
        SELECT * FROM {{ ref('stg_delta_employee_new') }}
        WHERE new_employee.{{col_new}} = {{value}}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
        {% endif %}
    {% endfor %}
{% endfor -%}
)
select * from result