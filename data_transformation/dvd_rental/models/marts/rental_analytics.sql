{{
    config(
        materialized='table',
        tags=['bi', 'mart', 'analytics']
    )
}}

-- Final denormalized table for BI consumption
-- Combines all dimensions and facts for easy querying

with fact_rentals as (
    select * from {{ ref('fct_rental') }}
),

dim_customers as (
    select * from {{ ref('dim_customer') }}
),

dim_films as (
    select * from {{ ref('dim_film') }}
),

dim_stores as (
    select * from {{ ref('dim_store') }}
),

dim_rental_dates as (
    select * from {{ ref('dim_date') }}
),

final as (
    select
        -- Fact identifiers
        f.rental_id,
        f.payment_id,
        f.inventory_id,
        
        -- Customer information
        c.customer_id,
        c.full_name as customer_name,
        c.first_name as customer_first_name,
        c.last_name as customer_last_name,
        c.email as customer_email,
        c.is_active as is_active_customer,
        c.customer_location,
        c.city_name as customer_city,
        c.country_name as customer_country,
        c.postal_code as customer_postal_code,
        c.phone as customer_phone,
        
        -- Film information
        fm.film_id,
        fm.title as film_title,
        fm.description as film_description,
        fm.release_year,
        fm.rating as film_rating,
        fm.category_name as film_category,
        fm.length_minutes as film_length_minutes,
        fm.film_length_category,
        fm.rental_rate as film_rental_rate,
        fm.rental_rate_category,
        fm.replacement_cost as film_replacement_cost,
        
        -- Store information
        s.store_id,
        s.store_location,
        s.city_name as store_city,
        s.country_name as store_country,
        
        -- Date information - Rental Date
        rd.date_day as rental_date,
        rd.year as rental_year,
        rd.month as rental_month,
        rd.quarter as rental_quarter,
        rd.day_name as rental_day_name,
        rd.month_name as rental_month_name,
        rd.is_weekend as is_rental_weekend,
        rd.is_weekday as is_rental_weekday,
        
        -- Transaction dates
        f.return_date,
        f.payment_date,
        
        -- Measures
        f.payment_amount,
        f.expected_amount,
        f.actual_payment,
        f.payment_variance,
        f.rental_duration_days as actual_rental_days,
        f.expected_rental_duration as expected_rental_days,
        f.days_overdue,
        f.calculated_late_fee,
        
        -- Business flags
        f.is_not_returned,
        f.is_late_return,
        
        -- Calculated metrics for BI
        case
            when f.is_not_returned then 'Not Returned'
            when f.is_late_return then 'Late Return'
            else 'On Time'
        end as return_status,
        
        case
            when f.payment_amount > f.expected_amount then 'Overpaid'
            when f.payment_amount < f.expected_amount then 'Underpaid'
            when f.payment_amount = f.expected_amount then 'Exact'
            else 'No Payment'
        end as payment_status,
        
        -- Revenue calculations
        f.actual_payment + f.calculated_late_fee as total_revenue,
        
        -- Profitability indicators
        case
            when (f.actual_payment + f.calculated_late_fee) >= f.expected_amount then true
            else false
        end as is_profitable_rental,
        
        -- Customer lifetime value indicators
        1 as rental_count,  -- For aggregations
        
        -- Metadata
        f.rental_last_update
        
    from fact_rentals f
    left join dim_customers c on f.customer_id = c.customer_id
    left join dim_films fm on f.film_id = fm.film_id
    left join dim_stores s on f.store_id = s.store_id
    left join dim_rental_dates rd on f.rental_date_id = rd.date_day
)

select * from final


