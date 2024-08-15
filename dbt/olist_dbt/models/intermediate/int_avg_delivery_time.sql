
with order_delivery_hours  as (
    select o.order_id,
    DATETIME_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, HOUR) as delivery_time,
    from {{ref('stg_orders')}} o


)
select order_id,
avg(delivery_time)/24 as avg_delivery_time_per_order
from order_delivery_hours
GROUP BY order_id