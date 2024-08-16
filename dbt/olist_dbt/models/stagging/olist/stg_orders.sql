
with raw_orders as (
    select 
    order_id, 
    customer_id, 
    order_status, 
    order_purchase_timestamp, 
    order_approved_at, 
    order_delivered_carrier_date,
    order_delivered_customer_date, 
    order_estimated_delivery_date
    from {{ source("olist","orders") }}
    where order_status = 'delivered' and  order_delivered_customer_date is not null

)

select * from raw_orders