{{ config(enabled=false) }}
-- Test that ensures inventory balances correctly
-- Checks that beginning_inventory + inventory_receipts - total_depletions + inventory_adjustments = ending_inventory
-- Allows for a small rounding tolerance of 0.01
-- A valid test should return zero rows

select 
    distributor_id,
    year_month,
    sku_id,
    beginning_inventory,
    inventory_receipts,
    total_depletions,
    inventory_adjustments,
    ending_inventory,
    (beginning_inventory + inventory_receipts - total_depletions + coalesce(inventory_adjustments, 0)) as calculated_ending_inventory,
    abs((beginning_inventory + inventory_receipts - total_depletions + coalesce(inventory_adjustments, 0)) - ending_inventory) as difference
from {{ ref('stg_vip__depletions') }}
where 
    -- Only check rows where we have all necessary values
    beginning_inventory is not null
    and inventory_receipts is not null
    and total_depletions is not null
    and ending_inventory is not null
    -- Allow for a small rounding tolerance (0.01)
    and abs((beginning_inventory + inventory_receipts - total_depletions + coalesce(inventory_adjustments, 0)) - ending_inventory) > 0.01 