with product_details as (
    select
    *
    from {{ source('mr_tables', 'products') }}
)