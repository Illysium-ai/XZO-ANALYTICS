{{
  config(
    materialized = 'view'
  )
}}

with deduped as (
select
  *,
  trim(split_part(brand, '-', 2)) as vip_brand_desc_stg,
  trim(split_part(variant, '-', 2)) as vip_variant_stg,
  right(variant_id, 5) as vip_variant_id,
  -- Extract the number of units before the "x"
  REGEXP_SUBSTR(size_pack, '^(\\\\d+)', 1, 1, 'e', 1)::int AS units,
  -- Extract the volume (in cL) after the "x" and multiply by 10 to get mL
  round(REGEXP_SUBSTR(size_pack, 'x(\\\\d+\\\\.?\\\\d*)', 1, 1, 'e', 1)::numeric * 10) AS volume_ml,
  ROW_NUMBER() OVER(PARTITION BY sku_id ORDER BY sku_id) AS row_num
from {{ source('master_data', 'hyperion_sku_master') }}
)
, ml_per_case as (
select
  *,
  -- For matching with VIP SKU - normalize using the macro
  left(vip_variant_id, 2) as vip_brand_id,
  {{ normalize_text('vip_brand_desc_stg') }} as vip_brand_desc,
  {{ normalize_text('vip_variant_stg') }} as vip_variant,
  units * volume_ml as ml_per_case
from deduped where row_num = 1
)
select
  brand,
  variant,
  size_pack,
  hyperion_sku,
  hp_coding,
  sku_id,
  'A' as sku_status,
  size_pack_id,
  variant_id,
  brand_id,
  vip_brand_desc,
  vip_brand_id,
  vip_variant,
  vip_variant_id,
  CONCAT(vip_variant_id, ' - ', vip_variant, ' - ', volume_ml) as vip_variant_size_desc,
  CONCAT(vip_variant_id, ' - ', vip_variant, ' ', units, 'x', volume_ml) as vip_variant_size_pack_desc,
  CONCAT(vip_variant_id, '-', volume_ml) as vip_variant_size_id,
  CONCAT(vip_variant_id, '-', units, '-', volume_ml) as vip_variant_size_pack_id,
  units,
  volume_ml,
  CONCAT(volume_ml, 'ML') as unit_volume_desc,
  ml_per_case,
  {{ case_equivalent_factor('ml_per_case') }} as case_equivalent_factor
from ml_per_case