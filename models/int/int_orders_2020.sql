with od as (
    select * from {{ ref('stg_order_details') }}
),

ds as (
    select * from {{ ref('stg_date_spine')}}
)

select 
    date(order_date_time) as date_day,
    order_id,
    Discounted_amount as Order_amount
from od
right join ds
on date(od.order_date_time) = date(ds.date_day)
order by ds.date_day