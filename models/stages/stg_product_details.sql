with product_details as (
    select
    productid as product_id,
    productname as product_name,
    productcostprice as product_price,
    productsaleprice as product_sale_price
    from {{ source('mr_tables', 'products') }}
)
select * from product_details