{{
    config(
        materialized='table',
        tags=['dimension', 'mart']
    )
}}

-- Generate a date dimension table from rental and payment dates
with date_spine as (
    select distinct date(rental_date) as date_day
    from {{ ref('stg_rental') }}
    
    union
    
    select distinct date(payment_date) as date_day
    from {{ ref('stg_payment') }}
),

final as (
    select
        date_day,
        
        -- Date parts
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(quarter from date_day) as quarter,
        extract(dayofweek from date_day) as day_of_week,
        extract(dayofyear from date_day) as day_of_year,
        extract(week from date_day) as week_of_year,
        
        -- Formatted dates
        date_format(date_day, 'yyyy-MM') as year_month,
        date_format(date_day, 'yyyy-QQQ') as year_quarter,
        
        -- Day name and month name
        date_format(date_day, 'EEEE') as day_name,
        date_format(date_day, 'MMMM') as month_name,
        
        -- Business logic flags
        case
            when extract(dayofweek from date_day) in (1, 7) then true
            else false
        end as is_weekend,
        
        case
            when extract(dayofweek from date_day) between 2 and 6 then true
            else false
        end as is_weekday,
        
        case
            when extract(day from date_day) = 1 then true
            else false
        end as is_month_start,
        
        case
            when extract(day from last_day(date_day)) = extract(day from date_day) then true
            else false
        end as is_month_end
        
    from date_spine
)

select * from final
order by date_day


