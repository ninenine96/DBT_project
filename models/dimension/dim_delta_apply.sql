{{
  config(
    materialized = 'table',
    )
}}

with source as (
    select * from {{ ref('int_delta_employee') }}
),

updated_table as (
    select * from source
    where is_updated in ('No Update','Updated Record','New Insertion')
)

select * from updated_table