{% set colname_old = get_column_names(table_name = ref('stg_delta_employee_all'), prefix = 'source.') %}
{% set colname_new = get_column_names(table_name = ref('stg_delta_employee_new'), prefix = 'destination.') %}
{% set colname_old1 = get_column_names(table_name = ref('stg_delta_employee_all'), prefix = 'destination.') %}
{% set colname_new1 = get_column_names(table_name = ref('stg_delta_employee_new'), prefix = 'source.') %}

with employees_all as (
    select * from {{ ref('stg_delta_employee_all') }}
),

employees_new as (
    select * from {{ ref('stg_delta_employee_new') }}
),

merge_tables as (
    select 
        {% for col in colname_old%}
            {{col}}{{','}}
        {% endfor %}
        case
            when source.emp_id = destination.emp_id and ({% for i in range(1, 4) %}
                                                               {{ colname_old[i] }}  != {{ colname_new[i] }}
                                                            {%if not loop.last%}
                                                                or 
                                                            {%endif%}
                                                         {% endfor %})
                
                
                then 'Outdated Record'
            else 'No Update'
        
        end as is_updated
    from {{ ref('stg_delta_employee_all') }} source
    left join employees_new destination
    on source.emp_id = destination.emp_id

    UNION 
    
    select 
        {% for col in colname_new1%}
            {{col}}{{','}}
        {% endfor %}
        case
            when source.emp_id = destination.emp_id and ({% for i in range(1, 4) %}
                                                               {{ colname_old1[i] }}  != {{ colname_new1[i] }}
                                                            {%if not loop.last%}
                                                                or 
                                                            {%endif%}
                                                         {% endfor %})
                then 'Updated Record'
            
            when {% for i in range(4) %}
                    {{ colname_old1[i] }}  != {{ colname_new1[i] }}
                    {%if not loop.last%}
                        and
                    {%endif%}
                {% endfor %} 
                then 'No update'
            
            else 'New Insertion'
        end as is_updated
    from employees_new source
    left join employees_all destination
    on destination.emp_id = source.emp_id

)

select * from merge_tables