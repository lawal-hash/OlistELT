
with order_delivery  as (
    select o.order_id,
    DATETIME_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY) as delivery_time,
    from {{ref('stg_orders')}} o


)
select order_id,
avg(delivery_time) as avg_delivery_time_per_order
from order_delivery
GROUP BY order_id