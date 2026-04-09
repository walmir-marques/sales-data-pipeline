with orders as (
    select * from {{ ref('fct_orders') }}
    WHERE order_date >= '2026-01-01' 
    AND order_date <= '2026-04-30'
),

monthly as (
    select
        date_trunc('month', order_date)::date   as month,
        count(order_id)                         as total_orders,
        sum(total_items)                        as total_items,
        sum(total_amount)                       as gross_revenue,
        sum(discounted_amount)                  as net_revenue,
        round(avg(discounted_amount), 2)        as avg_ticket
    from orders
    group by 1
)

select
    *,
    round(
        (net_revenue - lag(net_revenue) over (order by month))
        * 100.0
        / nullif(lag(net_revenue) over (order by month), 0),
    2) as mom_growth_pct
from monthly
order by month