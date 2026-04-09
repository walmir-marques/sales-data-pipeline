with source as (
    select * from {{ source('bronze', 'raw_users') }}
),

cleaned as (
    select
        id                              as user_id,
        first_name,
        last_name,
        first_name || ' ' || last_name  as full_name,
        lower(email)                    as email,
        phone,
        age,
        gender,
        city,
        state,
        _ingested_at
    from source
    where id is not null
      and email is not null
)

select * from cleaned