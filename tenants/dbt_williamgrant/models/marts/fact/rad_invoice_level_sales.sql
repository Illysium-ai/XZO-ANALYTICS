{{
  config(
    materialized = 'table'
  )
}}

-- Invoice Level Sales Detail view for drilling down on specific months/regions
select
  s.invoice_date,
  s.invoice_number,
  s.year,
  s.month,
  s.month_name,
  s.month_date,
  s.market_name,
  s.market_code,
  s.distributor_id,
  s.distributor_name,
  s.distributor_state,
  s.distributor_area,
  s.distributor_division_name,
  s.outlet_id,
  s.outlet_name,
  s.address_line_1,
  s.city,
  s.state,
  s.country,
  s.vip_chain_code,
  s.vip_chain_name,
  s.vip_parent_chain_code,
  s.vip_parent_chain_name,
  s.vip_cot_premise_type_code,
  s.vip_cot_premise_type_desc,
  s.sku_id,
  s.sku_description,
  s.brand_category,
  s.brand,
  s.brand_id,
  s.variant,
  s.variant_id,
  s.standardized_unit_volume_ml,
  s.variant_size_pack_desc,
  s.variant_size_pack_id,
  sum(s.phys_quantity) as phys_quantity,
  sum(s.case_equivalent_quantity) as case_equivalent_quantity,
  sum(s.net_amount) as sales_dollars,
  sum(s.frontline_amount) as frontline_sales_dollars
from 
  {{ ref('rad_sales_fact') }} s
where s.month_date >= (
  select date_trunc('year', max(month_date) - INTERVAL '2 YEAR')::DATE
  from {{ ref('rad_sales_fact') }}
  where month_date > current_date - INTERVAL '3 MONTH'
)
group by
  all