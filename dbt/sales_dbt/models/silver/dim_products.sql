with source as (
    select * from {{ source('bronze', 'raw_products') }}
),

cleaned as (
    select
        id                                      as product_id,
        title                                   as product_name,
        category,
        brand,
        price::numeric(10,2)                    as price,
        discount_percentage::numeric(5,2)       as discount_pct,
        round(
            price * (1 - discount_percentage / 100), 2
        )::numeric(10,2)                        as discounted_price,
        rating::numeric(3,2)                    as rating,
        stock                                   as stock_qty,
        _ingested_at
    from source
    where id is not null
      and price > 0
)

select * from cleaned