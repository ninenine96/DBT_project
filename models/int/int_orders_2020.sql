select 
    date(order_date_time)
from {{ ref('stg_order_details') }}
right join {{ ref('date_spine')}}
on date(stg_order_details.order_date_time) = date(date_spine.day_date)  git config --global user.email "you@example.com"
  git config --global user.name "Your Name"