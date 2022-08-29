with customer_details as (
    select * from {{ ref('stg_customer_details') }}
),

order_details as (
    select * from {{ ref('stg_order_details') }}
),

product_details as (
    select * from {{ ref('stg_product_details') }}
),

dim_order_table as (
    select 
        order_id,
        product_details.product_id,
        product_details.product_name,
        Discounted_amount,
        customer_details.customer_id,
        concat(customer_details.customer_first_name,' ',customer_details.customer_last_name) as name        
    from order_details
    left join product_details
    on product_details.product_id = order_details.product_id
    left join customer_details
    on customer_details.customer_id = order_details.customer_id
)

select * from dim_order_table