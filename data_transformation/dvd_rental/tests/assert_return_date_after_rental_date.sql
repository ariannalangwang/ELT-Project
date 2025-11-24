-- Test to ensure return dates are always after rental dates
-- This test will fail if any return date is before the rental date

select
    rental_id,
    rental_date,
    return_date
from {{ ref('fct_rental') }}
where return_date is not null
  and return_date < rental_date


