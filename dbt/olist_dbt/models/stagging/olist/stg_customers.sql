with raw_customers as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as customer_zip_code,
        customer_city,
        customer_state
    from {{ source("olist","customers") }}
)

select * from raw_customers



