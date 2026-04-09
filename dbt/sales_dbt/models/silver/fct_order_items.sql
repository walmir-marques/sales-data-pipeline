with source as (
    select * from {{ source('bronze', 'raw_order_items') }}
),

cleaned as (
    select
        order_id,
        product_id,
        title                               as product_name,
        price::numeric(10,2)                as unit_price,
        quantity,
        total::numeric(10,2)                as total_amount,
        discount_percentage::numeric(5,2)   as discount_pct,
        discounted_total::numeric(10,2)     as discounted_amount,
        _ingested_at
    from source
    where order_id is not null
      and product_id is not null
      and quantity > 0
)

select * from cleaned