{{
    config(
        materialized='table',
        tags=['dimension', 'mart']
    )
}}

with customers as (
    select * from {{ ref('stg_customer') }}
),

addresses as (
    select * from {{ ref('stg_address') }}
),

cities as (
    select * from {{ ref('stg_city') }}
),

countries as (
    select * from {{ ref('stg_country') }}
),

stores as (
    select * from {{ ref('stg_store') }}
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.first_name || ' ' || c.last_name as full_name,
        c.email,
        c.is_active,
        c.create_date,
        c.store_id,
        
        -- Address information
        a.address_line1,
        a.address_line2,
        a.district,
        a.postal_code,
        a.phone,
        
        -- Geographic information
        ci.city_name,
        co.country_name,
        
        -- Store location
        concat(ci.city_name, ', ', co.country_name) as customer_location,
        
        -- Metadata
        c.last_update as customer_last_update
        
    from customers c
    left join addresses a on c.address_id = a.address_id
    left join cities ci on a.city_id = ci.city_id
    left join countries co on ci.country_id = co.country_id
)

select * from final


