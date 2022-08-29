select 
    discounted_amount,
    original_amount
from {{ ref('stg_order_details') }}
where original_amount < discounted_amount