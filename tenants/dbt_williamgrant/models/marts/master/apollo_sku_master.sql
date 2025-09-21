{{
  config(
    materialized = 'incremental',
    unique_key = 'sku_id',
    on_schema_change = 'sync_all_columns'
  )
}}

with staged as (
  select
    coalesce(itm.supplier_item,mas.sku_id) as sku_id,
    coalesce(itm.desc,mas.hyperion_sku) as sku_description,
    mas.brand as hp_brand,
    mas.variant as hp_variant,
    mas.size_pack as hp_size_pack,
    mas.hyperion_sku as hp_sku,
    mas.hp_coding,
    itm.generic_cat3 as category,
    {{ normalize_text('itm.brand_desc') }} as stg_brand,
    itm.brand_code as stg_brand_id,
    {{ normalize_text('itm.variant') }} as stg_variant,
    coalesce(itm.generic_cat2, left(itm.generic_cat5,5))::text as stg_variant_id,
    itm.units,
    concat(itm.units::text, 'x', itm.standardized_unit_volume_ml::text) as size_pack_desc,
    case when itm.supplier_item is null then mas.vip_variant_size_id
      else concat(coalesce(itm.generic_cat2, left(itm.generic_cat5,5)),'-', itm.standardized_unit_volume_ml::text) end as variant_size_id,
    case when itm.supplier_item is null then mas.vip_variant_size_pack_id
      else concat(coalesce(itm.generic_cat2, left(itm.generic_cat5,5)),'-', itm.units::text,'-', itm.standardized_unit_volume_ml::text) end as variant_size_pack_id,
    mas.vip_variant_size_desc,
    mas.vip_variant_size_pack_desc,
    coalesce(itm.status, mas.sku_status) as sku_status,
    itm.activation_date,
    itm.deactivation_date,
    itm.weight as weight,
    itm.alcohol_pct as abv,
    itm.vintage as vintage_year,
    coalesce(itm.unit_volume_desc, mas.unit_volume_desc) as unit_volume_desc,
    coalesce(itm.standardized_unit_volume_ml, mas.volume_ml) as standardized_unit_volume_ml,
    coalesce(itm.case_equivalent_factor, mas.case_equivalent_factor)::float as case_equivalent_factor,
    'Standard 9L Case' as case_equivalent_type,
    itm.case_gtin as case_gtin,
    itm.retail_gtin as retail_gtin,
    itm.supplier_item,
    itm.generic_cat2
  from {{ ref('stg_vip__itm2da') }} itm
  full outer join {{ ref('stg_hyperion__sku_master') }} mas
    on itm.supplier_item = mas.sku_id
)
, unify_variant as (
select
  *,
  first_value(stg_brand) over (partition by variant_size_pack_id order by sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as brand,
  first_value(stg_brand_id) over (partition by variant_size_pack_id order by sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as brand_id,
  first_value(stg_variant) over (partition by variant_size_pack_id order by sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as variant,
  first_value(stg_variant_id) over (partition by variant_size_pack_id order by sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as variant_id
from staged
)
, unify_variant_size_pack_desc as (
select
  *,
  concat(variant_id, ' - ', variant, ' - ', standardized_unit_volume_ml::text) as stg_variant_size_desc,
  concat(variant_id, ' - ', variant, ' ', units::text, 'x', standardized_unit_volume_ml::text) as stg_variant_size_pack_desc
from unify_variant
)
, dedupe_variant_desc as (
select
  new.sku_id,
  new.sku_description,
  new.hp_brand,
  new.hp_variant,
  new.hp_size_pack,
  new.hp_sku,
  new.hp_coding,
  first_value(new.category) over (partition by new.variant_size_pack_id order by new.sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as brand_category,
  new.brand,
  new.brand_id,
  new.variant,
  new.variant_id,
  new.units,
  new.size_pack_desc,
  first_value(new.stg_variant_size_desc) over (partition by new.variant_size_id order by new.sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as variant_size_desc,
  first_value(new.stg_variant_size_pack_desc) over (partition by new.variant_size_pack_id order by new.sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as variant_size_pack_desc,
  new.variant_size_id,
  new.variant_size_pack_id,
  new.sku_status,
  TRY_TO_DATE(NULLIF(new.activation_date, '00000000'), 'YYYYMMDD') as activation_date,
  TRY_TO_DATE(NULLIF(new.deactivation_date, '00000000'), 'YYYYMMDD') as deactivation_date,
  new.weight,
  new.abv,
  new.vintage_year,
  new.unit_volume_desc,
  new.standardized_unit_volume_ml,
  new.case_equivalent_factor,
  new.case_equivalent_type,
  new.case_gtin,
  new.retail_gtin
from unify_variant_size_pack_desc new
)

, ifs_products as (
SELECT
  ifs.item_number as sku_id,
  ifs.item_name as sku_description,
  mas.brand as hp_brand,
  mas.variant as hp_variant,
  mas.size_pack as hp_size_pack,
  mas.hyperion_sku as hp_sku,
  mas.hp_coding,
  null as brand_category,
  {{ normalize_text('ifs.brand_name') }} as brand,
  ifs.brand_key as brand_id,
  {{ normalize_text('ifs.label_name') }} as variant,
  ifs.label_key as variant_id,
  ifs.units_per_case::double precision as units,
  concat(ifs.units_per_case::varchar, 'x', round((ifs.size_volume::double precision * 1000))::varchar) as size_pack_desc,
  concat(ifs.label_key, ' - ', {{ normalize_text('ifs.label_name') }}, ' - ', round((ifs.size_volume::double precision * 1000))::varchar) as variant_size_desc,
  concat(ifs.label_key, ' - ', {{ normalize_text('ifs.label_name') }}, ' ', ifs.units_per_case::varchar, 'x', round((ifs.size_volume::double precision * 1000))::varchar) as variant_size_pack_desc,
  concat(ifs.label_key, '-', round((ifs.size_volume::double precision * 1000))::varchar) as variant_size_id,
  concat(ifs.label_key, '-', ifs.units_per_case::varchar, '-', round((ifs.size_volume::double precision * 1000))::varchar) as variant_size_pack_id,
  left(ifs.active_flag, 1) as sku_status,
  null::date as activation_date,
  null::date as deactivation_date,
  null::double precision as weight,
  ifs.abv::double precision as abv,
  ifs.vintage::double precision as vintage_year,
  concat(round((ifs.size_volume::double precision * 1000))::varchar, 'ML') as unit_volume_desc,
  round((ifs.size_volume::double precision * 1000))::double precision as standardized_unit_volume_ml,
  (ifs.size_volume::float * ifs.units_per_case::float) / 9.0::float as case_equivalent_factor,
  'Standard 9L Case' as case_equivalent_type,
  ifs.gtin::bigint as case_gtin,
  null::bigint as retail_gtin
FROM
  {{ source('ifs','products')}} ifs
left join {{ ref('stg_hyperion__sku_master') }} mas
  on ifs.item_number = mas.sku_id
)

, final_combined as (
  -- All rows from ifs_products
  select *, 1 as source_rank from ifs_products
  union all
  -- Only rows from dedupe_variant_desc that don't exist in ifs_products
  select *, 2 as source_rank from dedupe_variant_desc
  where not exists (select 1 from ifs_products where dedupe_variant_desc.sku_id = ifs_products.sku_id)
)
select
  itm.sku_id,
  itm.sku_description,
  itm.hp_brand,
  itm.hp_variant,
  itm.hp_size_pack,
  itm.hp_sku,
  itm.hp_coding,
  itm.brand_category,
  first_value(itm.brand) over (partition by itm.variant_size_pack_id order by itm.source_rank, itm.sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as brand,
  first_value(itm.brand_id) over (partition by itm.variant_size_pack_id order by itm.source_rank, itm.sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as brand_id,
  first_value(itm.variant) over (partition by itm.variant_size_pack_id order by itm.source_rank, itm.sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as variant,
  first_value(itm.variant_id) over (partition by itm.variant_size_pack_id order by itm.source_rank, itm.sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as variant_id,
  itm.units,
  itm.size_pack_desc,
  first_value(itm.variant_size_desc) over (partition by itm.variant_size_id order by itm.source_rank, itm.sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as variant_size_desc,
  first_value(itm.variant_size_pack_desc) over (partition by itm.variant_size_pack_id order by itm.source_rank, itm.sku_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as variant_size_pack_desc,
  itm.variant_size_id,
  itm.variant_size_pack_id,
  itm.sku_status,
  itm.activation_date,
  itm.deactivation_date,
  itm.weight,
  itm.abv,
  itm.vintage_year,
  itm.unit_volume_desc,
  itm.standardized_unit_volume_ml,
  itm.case_equivalent_factor,
  itm.case_equivalent_type,
  itm.case_gtin,
  itm.retail_gtin,
  nst.nielsen_variant_size,
  nst.jenda_variant_size,
  nst.nabca_variant_size
from final_combined itm
left join {{ source('master_data', 'nrm_system_translation') }} nst
on itm.sku_id = nst.part