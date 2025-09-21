{{
  config(
    materialized = 'incremental',
    on_schema_change = 'sync_all_columns',
    unique_key = ['distributor_id', 'alt_dist_id', 'year_month', 'period', 'sku_id', 'distributor_item_id'],
    enabled = false
  )
}}

{# {% set use_history_data = var('use_history_data', true) %} #}

with 
current_data as (
  select
    c.customer_id,
    c.distributor_id,
    c.year_month,
    c.period,
    c.sku_id,
    c.distributor_item_id,
    c.year,
    c.month,
    DATE_FROM_PARTS(c.year, c.month, 1) as month_date,
    c.beginning_inventory,
    c.inventory_receipts,
    c.inventory_adjustments,
    c.inventory_trans_in,
    c.inventory_trans_out,
    c.inventory_returns,
    c.inventory_breakage,
    c.inventory_samples,
    c.inventory_total_chg,
    c.ending_inventory,
    c.on_order_inventory,
    c.total_depletions,
    c.non_retail_depletions,
    c.off_premise_depletions,
    c.on_premise_depletions,
    c.military_on_premise,
    c.military_off_premise,
    c.sub_distributor_sales,
    c.transport_depletions,
    c.unclassified_depletions,
    c.days_of_supply,
    c.depletion_flow,
    c.audit_date,
    c.audit_user,
    c.out_audit_date,
    c.out_audit_user,
    c.alt_dist_id
  from 
    {{ ref('stg_vip__depletions') }} c
  {% if is_incremental() %}
    where c.month_date >= (select (max(month_date)- INTERVAL '3 MONTH')::DATE from {{ this }})
  {% endif %}
),

select
  cd.customer_id,
  cd.distributor_id,
  dist.distributor_name,
  cd.year_month,
  cd.period,
  cd.sku_id,
  cd.distributor_item_id,
  dist.state,
  dist.market_name,
  dist.market_code,
  -- Time components
  cd.year,
  cd.month,
  cd.month_date,
  -- Inventory positions
  cd.beginning_inventory,
  cd.inventory_receipts,
  cd.inventory_adjustments,
  cd.inventory_trans_in,
  cd.inventory_trans_out,
  cd.inventory_returns,
  cd.inventory_breakage,
  cd.inventory_samples,
  cd.inventory_total_chg,
  cd.ending_inventory,
  cd.on_order_inventory,
  -- Sales breakdowns (depletions)
  cd.total_depletions,
  cd.non_retail_depletions,
  cd.off_premise_depletions,
  cd.on_premise_depletions,
  cd.military_on_premise,
  cd.military_off_premise,
  cd.sub_distributor_sales,
  cd.transport_depletions,
  cd.unclassified_depletions,
  -- Days of supply calculation
  cd.days_of_supply,
  -- Industry tier indicators
  cd.depletion_flow,
  -- Additional key fields
  cd.alt_dist_id,
  -- Product information
  p.brand_category,
  p.brand,
  p.brand_id,
  p.variant,
  p.variant_id,
  p.variant_size_pack_desc,
  p.variant_size_pack_id,
  p.standardized_unit_volume_ml,
  p.case_equivalent_factor,
  -- Computed case equivalents for normalization across products
  cd.total_depletions * coalesce(p.case_equivalent_factor, 1.0) as case_equivalent_depletions,
  cd.beginning_inventory * coalesce(p.case_equivalent_factor, 1.0) as case_equivalent_begin_inventory,
  cd.ending_inventory * coalesce(p.case_equivalent_factor, 1.0) as case_equivalent_end_inventory,
  -- Track audit information
  cd.audit_date,
  cd.audit_user,
  cd.out_audit_date,
  cd.out_audit_user
from 
  current_data cd
left join 
  {{ ref('apollo_sku_master') }} p on cd.sku_id = p.sku_id
left join
  {{ ref('apollo_dist_master') }} dist on cd.distributor_id = dist.distributor_id