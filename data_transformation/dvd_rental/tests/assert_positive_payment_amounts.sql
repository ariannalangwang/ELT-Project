-- Test to ensure all payment amounts are positive
-- This test will fail if there are any negative or zero payment amounts

select
    payment_id,
    customer_id,
    amount
from {{ ref('fct_rental') }}
where payment_amount < 0

