{{ config(enabled=false) }}

-- Test that ensures case equivalent factors are always positive numbers
-- Returns rows where case_equivalent_factor is not a positive number
-- A valid test should return zero rows

select *
from {{ ref('apollo_sku_master') }}
where case_equivalent_factor is not null
  and case_equivalent_factor <= 0 