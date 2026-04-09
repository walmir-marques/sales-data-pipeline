with order_items as (
    select * from "sales_db"."silver"."fct_order_items"
),

products as (
    select * from "sales_db"."silver"."dim_products"
),

aggregated as (
    select
        p.category,
        count(distinct oi.order_id)  as total_orders,
        sum(oi.quantity)             as total_units_sold,
        sum(oi.total_amount)         as gross_revenue,
        sum(oi.discounted_amount)    as net_revenue,
        round(avg(oi.unit_price), 2) as avg_unit_price,
        count(distinct oi.product_id) as total_products
    from order_items oi
    left join products p on oi.product_id = p.product_id
    group by 1
)

select
    *,
    round(net_revenue * 100.0 / sum(net_revenue) over (), 2) as revenue_share_pct
from aggregated
order by net_revenue desc