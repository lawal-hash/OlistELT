
with order_by_state as (
    select o.order_id,
    c.customer_state,
    c.customer_city
    from {{ref('stg_orders')}} o
    join {{ ref('stg_customers')}} c
    using (customer_id)

)
select customer_state,
count(order_id) as total_orders,
from order_by_state
GROUP BY customer_state