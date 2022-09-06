with full_table as (
    select * from {{ ref('int_delta_employee') }}
),

updated_records as (
    select * from {{ ref('stg_delta_employee_new') }}
),

updated_entries as (
    select U.emp_id, U.name, U.occupation, U.record_date, is_updated as Is_Updated
    from updated_records U
    left join full_table
    on U.emp_id = full_table.emp_id
    where date(U.record_date) = '2022-09-05'
),

new_entries as (
    select * from  updated_records
    except
    select U.emp_id, U.name, U.occupation, U.record_date
    from updated_entries U
)


select *
from new_entries N
right join
N.emp_id NOT U.emp_id
select * from updated_entries U