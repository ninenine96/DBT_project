{% set cols_old = dbt_utils.star(from=ref('stg_delta_employee_all')) %}
{% set cols_new = [ dbt_utils.star(from=ref('stg_delta_employee_new')) ] %}

with employees_all as (
    select * from {{ ref('stg_delta_employee_all') }}
),

employees_new as (
    select * from {{ ref('stg_delta_employee_new') }}
),

merge_tables as (
    select {{ dbt_utils.star(from=ref('stg_delta_employee_all'), prefix='source.')}} ,
        case
            when source.emp_id = destination.emp_id and (source.occupation != destination.occupation 
                                                        or source.record_date != destination.record_date 
                                                        or source.name != destination.name)
                then 'Outdated Record'
            else 'No Update'
        end as is_updated
    from {{ ref('stg_delta_employee_all') }} source
    left join employees_new destination
    on source.emp_id = destination.emp_id

    UNION 
    
    select source.emp_id, source.name, source.occupation, source.record_date,
        case
            when source.emp_id = destination.emp_id and (source.occupation != destination.occupation 
                                                        or source.record_date != destination.record_date 
                                                        or source.name != destination.name)
                then 'Updated Record'
            
            when destination.emp_id = source.emp_id 
                    and source.occupation = destination.occupation 
                    and source.record_date = destination.record_date 
                    and source.name = destination.name
                then 'No update'
            
            else 'New Insertion'
        end as is_updated
    from employees_new source
    left join employees_all destination
    on destination.emp_id = source.emp_id

)

select * from merge_tables