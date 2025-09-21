{{
  config(
    materialized = 'table'
  )
}}

-- Distributor Level Sales Detail view for drilling down on specific months/regions
select
  src.year,
  src.month,
  src.month_name,
  src.month_date,
  src.market_name,
  src.market_code,
  src.distributor_id,
  src.distributor_name,
  src.distributor_state,
  src.distributor_area,
  src.distributor_division_name,
  src.brand,
  src.brand_id,
  src.variant,
  src.variant_id,
  src.variant_size_pack_desc,
  src.variant_size_pack_id,
  sum(src.phys_quantity) as phys_quantity,
  sum(src.case_equivalent_quantity) as case_equivalent_quantity,
  sum(src.sales_dollars) as sales_dollars,
  sum(src.frontline_sales_dollars) as frontline_sales_dollars
from 
  {{ ref('rad_invoice_level_sales') }} src
  group by all