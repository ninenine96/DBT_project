{{
    config(
        materialized = 'delta_apply',
        primary_key = 'emp_id',
        target_table = 'STG_DELTA_SOURCE',
    )
}}

{# {{
  config(
    materialized = 'table',
    )
}} #}

with new as (
    select * from {{ source('mr_tables', 'DELTA_EMPLOYEE_NEW') }}
)

select * from new