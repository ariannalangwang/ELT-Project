{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('dvd_rental', 'inventory') }}
),

cleaned as (
    select
        inventory_id,
        film_id,
        store_id,
        last_update
    from source
)

select * from cleaned

