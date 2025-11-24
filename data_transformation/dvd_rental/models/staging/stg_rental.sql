{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('dvd_rental', 'rental') }}
),

cleaned as (
    select
        rental_id,
        rental_date,
        inventory_id,
        customer_id,
        return_date,
        staff_id,
        last_update,
        
        -- Calculate rental duration in days
        case
            when return_date is not null then
                datediff(day, rental_date, return_date)
            else null
        end as rental_duration_days
        
    from source
)

select * from cleaned


