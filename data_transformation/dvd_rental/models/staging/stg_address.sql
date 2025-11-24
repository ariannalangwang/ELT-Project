{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('dvd_rental', 'address') }}
),

cleaned as (
    select
        address_id,
        address as address_line1,
        address2 as address_line2,
        district,
        city_id,
        postal_code,
        phone,
        last_update
    from source
)

select * from cleaned

