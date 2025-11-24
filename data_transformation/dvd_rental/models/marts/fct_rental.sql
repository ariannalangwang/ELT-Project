{{
    config(
        materialized='table',
        tags=['fact', 'mart']
    )
}}

with rentals as (
    select * from {{ ref('stg_rental') }}
),

payments as (
    select * from {{ ref('stg_payment') }}
),

inventory as (
    select * from {{ ref('stg_inventory') }}
),

films as (
    select * from {{ ref('stg_film') }}
),

final as (
    select
        -- Fact table surrogate key
        r.rental_id,
        
        -- Foreign keys to dimensions
        r.customer_id,
        i.film_id,
        i.store_id,
        r.staff_id,
        date(r.rental_date) as rental_date_id,
        date(r.return_date) as return_date_id,
        date(p.payment_date) as payment_date_id,
        
        -- Degenerate dimensions (transaction details)
        r.rental_date,
        r.return_date,
        p.payment_date,
        r.inventory_id,
        p.payment_id,
        
        -- Measures
        p.amount as payment_amount,
        f.rental_rate as expected_amount,
        r.rental_duration_days,
        f.rental_duration as expected_rental_duration,
        
        -- Derived measures
        coalesce(p.amount, 0) as actual_payment,
        coalesce(p.amount, 0) - f.rental_rate as payment_variance,
        
        -- Business flags
        case
            when r.return_date is null then true
            else false
        end as is_not_returned,
        
        case
            when r.return_date is not null 
                and r.rental_duration_days > f.rental_duration then true
            else false
        end as is_late_return,
        
        case
            when r.return_date is not null 
                and r.rental_duration_days > f.rental_duration
            then r.rental_duration_days - f.rental_duration
            else 0
        end as days_overdue,
        
        -- Calculated late fees (assuming $1 per day overdue)
        case
            when r.return_date is not null 
                and r.rental_duration_days > f.rental_duration
            then (r.rental_duration_days - f.rental_duration) * 1.0
            else 0
        end as calculated_late_fee,
        
        -- Metadata
        r.last_update as rental_last_update
        
    from rentals r
    left join payments p on r.rental_id = p.rental_id
    left join inventory i on r.inventory_id = i.inventory_id
    left join films f on i.film_id = f.film_id
)

select * from final


