-- Test to ensure all rentals have valid customers
-- This test will fail if there are rentals without matching customers

select
    f.rental_id,
    f.customer_id
from {{ ref('fct_rental') }} f
left join {{ ref('dim_customer') }} c
    on f.customer_id = c.customer_id
where c.customer_id is null

