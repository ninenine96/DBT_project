with customer_details as (
    select 
        customerid as customer_id,
        customerfirstname as customer_first_name,
        customerlastname as customer_last_name,
        customerlocation as customer_locatiom,
        customerdob as customer_dob,
        customermemberfrom as customer_membership_date
    from {{ source('mr_tables', 'customers') }}
)

select * from customer_details