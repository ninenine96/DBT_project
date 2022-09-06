with order_details as (
    select 
        orderid as order_id,
        orderdatetime as order_date_time,
        orderisdelivery as order_is_delivery,
        orderisdiscount as order_is_discount,
        orderamountincdiscount as Discounted_amount,
        orderamountexcdiscount as Original_amount,
        productid as product_id,
        customerid as customer_id
    from {{ source('mr_tables', 'orders') }}
)
select * from order_details
