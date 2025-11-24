{{
    config(
        materialized='table',
        tags=['dimension', 'mart']
    )
}}

with stores as (
    select * from {{ ref('stg_store') }}
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

final as (
    select
        s.store_id,
        s.manager_staff_id,
        
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
        concat(ci.city_name, ', ', co.country_name) as store_location,
        
        -- Metadata
        s.last_update as store_last_update
        
    from stores s
    left join addresses a on s.address_id = a.address_id
    left join cities ci on a.city_id = ci.city_id
    left join countries co on ci.country_id = co.country_id
)

select * from final

