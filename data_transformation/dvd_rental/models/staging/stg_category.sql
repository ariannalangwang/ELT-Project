{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('dvd_rental', 'category') }}
),

cleaned as (
    select
        category_id,
        name as category_name,
        last_update
    from source
)

select * from cleaned

