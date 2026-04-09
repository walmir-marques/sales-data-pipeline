
  
    

  create  table "sales_db"."gold"."customer_summary__dbt_tmp"
  
  
    as
  
  (
    with orders as (
    select * from "sales_db"."silver"."fct_orders"
),

users as (
    select * from "sales_db"."silver"."dim_users"
),

aggregated as (
    select
        u.user_id,
        u.full_name,
        u.email,
        u.city,
        u.state,
        u.gender,
        u.age,
        count(o.order_id)               as total_orders,
        sum(o.total_amount)             as gross_spent,
        sum(o.discounted_amount)        as net_spent,
        round(avg(o.discounted_amount), 2) as avg_ticket,
        sum(o.total_items)              as total_items_bought
    from users u
    left join orders o on u.user_id = o.user_id
    group by 1, 2, 3, 4, 5, 6, 7
)

select
    *,
    rank() over (order by net_spent desc) as spending_rank
from aggregated
order by net_spent desc
  );
  