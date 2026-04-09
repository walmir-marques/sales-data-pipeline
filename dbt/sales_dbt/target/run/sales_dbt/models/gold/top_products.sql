
  
    

  create  table "sales_db"."gold"."top_products__dbt_tmp"
  
  
    as
  
  (
    with order_items as (
    select * from "sales_db"."silver"."fct_order_items"
),

products as (
    select * from "sales_db"."silver"."dim_products"
),

aggregated as (
    select
        oi.product_id,
        oi.product_name,
        p.category,
        p.brand,
        sum(oi.quantity)            as total_units_sold,
        sum(oi.total_amount)        as gross_revenue,
        sum(oi.discounted_amount)   as net_revenue,
        round(avg(oi.unit_price), 2) as avg_unit_price,
        count(distinct oi.order_id) as total_orders
    from order_items oi
    left join products p on oi.product_id = p.product_id
    group by 1, 2, 3, 4
)

select
    *,
    rank() over (order by net_revenue desc) as revenue_rank
from aggregated
order by net_revenue desc
  );
  