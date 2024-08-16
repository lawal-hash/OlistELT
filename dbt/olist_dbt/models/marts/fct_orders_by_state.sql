

with order_by_state  as (
    select 
    customer_state,
    total_orders,
    rank() over (order by total_orders desc) as category_rank
    from {{ref('int_orders_by_state')}} 


)

select customer_state, total_orders
from order_by_state
where category_rank <= 5