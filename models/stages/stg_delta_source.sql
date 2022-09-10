{{
  config(
    materialized = 'table',
    )
}}

with source as (
    select * from {{ source('mr_tables', 'DELTA_EMPLOYEE_SOURCE') }}
)

select * from source