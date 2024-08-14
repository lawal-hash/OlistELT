with raw_products as (
    select 
    product_id, 
    product_category_name
    from {{source("olist","products")}}
    where product_category_name is not null

)
select * from raw_products