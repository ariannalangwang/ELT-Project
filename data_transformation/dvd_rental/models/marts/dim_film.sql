{{
    config(
        materialized='table',
        tags=['dimension', 'mart']
    )
}}

with films as (
    select * from {{ ref('stg_film') }}
),

film_categories as (
    select * from {{ ref('stg_film_category') }}
),

categories as (
    select * from {{ ref('stg_category') }}
),

final as (
    select
        f.film_id,
        f.title,
        f.description,
        f.release_year,
        f.language_id,
        f.rental_duration,
        f.rental_rate,
        f.length_minutes,
        f.replacement_cost,
        f.rating,
        
        -- Category information
        c.category_name,
        
        -- Derived attributes
        case
            when f.length_minutes < 60 then 'Short'
            when f.length_minutes between 60 and 120 then 'Medium'
            else 'Long'
        end as film_length_category,
        
        case
            when f.rental_rate < 1.0 then 'Budget'
            when f.rental_rate between 1.0 and 2.99 then 'Standard'
            when f.rental_rate between 3.0 and 4.99 then 'Premium'
            else 'Luxury'
        end as rental_rate_category,
        
        -- Metadata
        f.last_update as film_last_update
        
    from films f
    left join film_categories fc on f.film_id = fc.film_id
    left join categories c on fc.category_id = c.category_id
)

select * from final

