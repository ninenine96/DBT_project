with order_details as (
    select * from
    {{ ref('stg_order_details') }}
),

customer_details as (
    select * from
    {{ ref('stg_customer_details') }}
),
customer_orders as (
    select 
        order_id,
        Discounted_amount,
        customer_details.customer_id,
        customer_details.customer_first_name,
        customer_details.customer_last_name,
        customer_details.customer_dob
    from customer_details
    left join order_details
    on order_details.customer_id = customer_details.customer_id
)

select * from customer_orders
