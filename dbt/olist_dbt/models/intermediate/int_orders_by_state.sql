
with order_by_state as (
    select o.order_id,
    c.customer_state,
    c.customer_city
    from {{ref('stg_orders')}} o
    join {{ ref('stg_customers')}} c
    using (customer_id)

), count_order_by_state as (
select customer_state,
count(order_id) as total_orders,
from order_by_state
GROUP BY customer_state)

select 
    s.state as customer_state,
    cs.total_orders 
from count_order_by_state cs
join {{ref('states')}} s
on cs.customer_state = s.code