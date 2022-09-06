with delta_employees_new as (
    select 
        emp_id,
        concat(first_name,' ',last_name) as name,
        occupation,
        record_date
    from {{source('mr_tables', 'DELTA_EMPLOYEE_59')}}
)

select * from delta_employees_new