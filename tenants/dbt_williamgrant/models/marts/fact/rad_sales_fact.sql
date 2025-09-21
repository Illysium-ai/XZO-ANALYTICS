{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    alias = 'rad_sales_daily_fact',
    on_schema_change = 'sync_all_columns',
    unique_key = ['invoice_date', 'distributor_id']
  )
}}

{# unique_key = ['month_date', 'distributor_id', 'outlet_id', 'sku_id', 'distributor_item_id', 'invoice_date', 'invoice_number', 'invoice_line'] #}

{% if is_incremental() %}
{% set sql_statement %}
  (select (max(month_date) - INTERVAL '6 MONTH')::DATE from {{ this }} where month_date > current_date - INTERVAL '3 MONTH')
{% endset %}
{% set max_month_date = dbt_utils.get_single_value(sql_statement) %}
{% endif %}

with 
current_data as (
  select
    c.year,
    c.month,
    c.month_name,
    c.month_date,
    c.distributor_id,
    c.outlet_id,
    c.sku_id,
    c.distributor_item_id,
    c.invoice_date,
    c.invoice_number,
    c.invoice_line,
    c.uom,
    c.quantity,
    c.net_amount,
    c.frontline_amount
  from 
    {{ ref('stg_vip__sales') }} c
  {% if is_incremental() %}
  where c.month_date >= '{{ max_month_date }}'
  {% endif %}
)

select
  cd.year,
  cd.month,
  cd.month_name,
  cd.month_date,
  cd.distributor_id,
  d.distributor_name,
  d.state as distributor_state,
  d.area_name as distributor_area,
  d.division_name as distributor_division_name,
  d.market_name,
  d.market_code,
  cd.outlet_id,
  o.outlet_name,
  o.address_line_1,
  o.city,
  o.state,
  o.country,
  o.vip_id,
  o.vip_chain_code,
  o.vip_chain_name,
  o.vip_parent_chain_code,
  o.vip_parent_chain_name,
  o.chain_store_num,
  o.is_franchise,
  o.chain_status,
  o.vip_cot_code,
  o.vip_cot_premise_type_code,
  o.vip_cot_premise_type_desc,
  o.ownership_hier_lvl1,
  o.ownership_hier_lvl2,
  o.ownership_hier_lvl3,
  o.ownership_hier_lvl4,
  o.ownership_status,
  o.ownership_hier_lvl1_name,
  o.ownership_hier_lvl2_name,
  o.ownership_hier_lvl3_name,
  o.ownership_hier_lvl4_name,
  cd.sku_id,
  cd.distributor_item_id,
  p.sku_description,
  p.brand_category,
  p.brand,
  p.brand_id,
  p.variant,
  p.variant_id,
  p.standardized_unit_volume_ml,
  p.variant_size_pack_desc,
  p.variant_size_pack_id,
  cd.invoice_date,
  cd.invoice_number,
  cd.invoice_line,
  coalesce(p.case_equivalent_factor, 1.0) as case_equivalent_factor,
  p.case_equivalent_type,
  -- For excise tax tracking
  coalesce(p.abv, 0) as product_abv,
  sum(case
    when cd.uom = 'C' then cd.quantity
    when cd.uom = 'B' then cd.quantity / p.units
    else cd.quantity
  end) as phys_quantity,
  sum(case
    when cd.uom = 'C' then cd.quantity
    when cd.uom = 'B' then cd.quantity / p.units
    else cd.quantity
  end) * coalesce(p.case_equivalent_factor, 1.0) as case_equivalent_quantity,
  sum(cd.net_amount) as net_amount,
  sum(cd.frontline_amount) as frontline_amount
from 
  current_data cd
left join 
  {{ ref('apollo_account_master') }} o on cd.outlet_id = o.outlet_id and cd.distributor_id = o.distributor_id
left join
  {{ ref('apollo_dist_master') }} d on cd.distributor_id = d.distributor_id
left join 
  {{ ref('apollo_sku_master') }} p on cd.sku_id = p.sku_id
where 
  --Ignore non-retail sales via COT code
  coalesce(o.vip_cot_code, '99') != '06'
  -- Ignore non-depletions eligible sales via is_depletions_eligible
  and d.is_depletions_eligible = 1
group by
  all
