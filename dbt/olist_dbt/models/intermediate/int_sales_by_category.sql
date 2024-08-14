
with sales as (
    select o.order_id,
    p.product_category_name,
    oi.price,
    from {{ref('stg_orders')}} o
    join {{ref('stg_order_items')}} oi 
    using (order_id)
    join {{ ref('stg_products')}} p
    using (product_id)

)
select product_category_name,
sum(price) as total_sales
from sales
GROUP BY product_category_name