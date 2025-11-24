-- Test to ensure total revenue calculation is consistent
-- This test will fail if calculated total_revenue doesn't match the sum of components

select
    rental_id,
    actual_payment,
    calculated_late_fee,
    total_revenue,
    (actual_payment + calculated_late_fee) as calculated_total
from {{ ref('rental_analytics') }}
where abs(total_revenue - (actual_payment + calculated_late_fee)) > 0.01

