{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('dvd_rental', 'film_category') }}
),

cleaned as (
    select
        film_id,
        category_id,
        last_update
    from source
)

select * from cleaned

