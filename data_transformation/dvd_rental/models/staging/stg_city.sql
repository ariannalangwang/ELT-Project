{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('dvd_rental', 'city') }}
),

cleaned as (
    select
        city_id,
        city as city_name,
        country_id,
        last_update
    from source
)

select * from cleaned


