with delta_employees_all as (
    select 
        emp_id,
        concat(first_name,' ',last_name) as name,
        occupation,
        record_date
    from {{source('mr_tables', 'DELTA_EMPLOYEE_ALL')}}
)

select * from delta_employees_all