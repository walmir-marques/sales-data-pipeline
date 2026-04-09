
  
    

  create  table "sales_db"."silver"."fct_orders__dbt_tmp"
  
  
    as
  
  (
    with source as (
    select * from "sales_db"."bronze"."raw_orders"
),

cleaned as (
    select
        id                              as order_id,
        user_id,
        total::numeric(10,2)            as total_amount,
        discounted_total::numeric(10,2) as discounted_amount,
        total_products                  as total_products,
        total_quantity                  as total_items,
        _ingested_at                    as order_date
    from source
    where id is not null
      and user_id is not null
)

select * from cleaned
  );
  