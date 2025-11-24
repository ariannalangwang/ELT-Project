{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('dvd_rental', 'country') }}
),

cleaned as (
    select
        country_id,
        country as country_name,
        last_update
    from source
)

select * from cleaned

