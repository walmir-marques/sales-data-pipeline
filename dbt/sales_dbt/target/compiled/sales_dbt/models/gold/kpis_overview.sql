with orders as (
    select * from "sales_db"."silver"."fct_orders"
),

items as (
    select * from "sales_db"."silver"."fct_order_items"
),

users as (
    select * from "sales_db"."silver"."dim_users"
),

kpis as (
    select
        count(distinct o.order_id)          as total_orders,
        count(distinct o.user_id)           as total_customers,
        sum(o.total_amount)                 as gross_revenue,
        sum(o.discounted_amount)            as net_revenue,
        round(avg(o.discounted_amount), 2)  as avg_ticket,
        sum(o.total_items)                  as total_items_sold,
        round(
            sum(o.total_amount - o.discounted_amount)
            * 100.0 / nullif(sum(o.total_amount), 0),
        2)                                  as avg_discount_pct
    from orders o
)

select * from kpis