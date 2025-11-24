{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('dvd_rental', 'film') }}
),

cleaned as (
    select
        film_id,
        title,
        description,
        cast(release_year as int) as release_year,
        language_id,
        rental_duration,
        rental_rate,
        length as length_minutes,
        replacement_cost,
        rating,
        last_update
    from source
)

select * from cleaned


