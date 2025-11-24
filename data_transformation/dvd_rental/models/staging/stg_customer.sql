{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('dvd_rental', 'customer') }}
),

cleaned as (
    select
        customer_id,
        store_id,
        first_name,
        last_name,
        email,
        address_id,
        activebool as is_active,
        create_date,
        last_update
    from source
)

select * from cleaned

