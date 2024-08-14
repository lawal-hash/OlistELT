with avg_order_delivery  as (
    select order_id,
    avg_delivery_time_per_order
    from {{ref('int_avg_delivery_time')}} 


)
select
avg(avg_delivery_time_per_order) as avg_delivery_time_model
from avg_order_delivery
