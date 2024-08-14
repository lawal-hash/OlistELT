
with raw_order_items as (
    select 
    order_id, 
    order_item_id, 
    product_id, 
    seller_id,
    shipping_limit_date,
    price, 
    freight_value

    from {{source("olist","order_items")}}

)
select * from raw_order_items