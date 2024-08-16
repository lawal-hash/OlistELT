with sales  as (
    select 
    product_category,
    total_sales,
    rank() over (order by total_sales desc) as category_rank
    from {{ref('int_sales_by_category')}} 


)

select product_category, total_sales
from sales
where category_rank = 1


