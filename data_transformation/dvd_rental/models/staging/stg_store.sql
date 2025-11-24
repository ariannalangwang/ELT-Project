{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('dvd_rental', 'store') }}
),

cleaned as (
    select
        store_id,
        manager_staff_id,
        address_id,
        last_update
    from source
)

select * from cleaned

