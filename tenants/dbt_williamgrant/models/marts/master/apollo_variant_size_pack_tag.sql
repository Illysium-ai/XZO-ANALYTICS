{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'variant_size_pack_id',
    on_schema_change = 'sync_all_columns',
    full_refresh = false
  )
}}

with source_vsp as (
  select distinct
    variant_size_pack_id,
    variant_size_pack_desc
  from {{ ref('apollo_sku_master') }}
  where variant_size_pack_id is not null
),
base_data as (
  select
    s.variant_size_pack_id,
    {% if is_incremental() %}
    coalesce(s.variant_size_pack_desc, t.variant_size_pack_desc) as variant_size_pack_desc,
    {% else %}
    s.variant_size_pack_desc as variant_size_pack_desc,
    {% endif %}
    {% if is_incremental() %}
    t.tag_ids,
    t.tag_names,
    coalesce(t.is_planned, 'default') as is_planned,
    t.market_code_exclusions,
    t.customer_id_exclusions,
    t.is_custom_product,
    t.custom_brand,
    t.custom_brand_id,
    t.custom_variant,
    t.custom_variant_id,
    t.custom_pack,
    t.custom_size_ml,
    t.remapped_from_variant_size_pack_id
    {% else %}
    cast(null as array) as tag_ids,
    cast(null as array) as tag_names,
    'default' as is_planned,
    cast(null as array) as market_code_exclusions,
    cast(null as array) as customer_id_exclusions,
    cast(null as boolean) as is_custom_product,
    cast(null as varchar) as custom_brand,
    cast(null as varchar) as custom_brand_id,
    cast(null as varchar) as custom_variant,
    cast(null as varchar) as custom_variant_id,
    cast(null as integer) as custom_pack,
    cast(null as integer) as custom_size_ml,
    cast(null as varchar) as remapped_from_variant_size_pack_id
    {% endif %}
  from source_vsp s
  {% if is_incremental() %}
  left join {{ this }} t
    on t.variant_size_pack_id = s.variant_size_pack_id
  {% endif %}
)

select
  variant_size_pack_id,
  variant_size_pack_desc,
  tag_ids,
  tag_names,
  is_planned,
  market_code_exclusions,
  customer_id_exclusions,
  is_custom_product,
  custom_brand,
  custom_brand_id,
  custom_variant,
  custom_variant_id,
  custom_pack,
  custom_size_ml,
  remapped_from_variant_size_pack_id
from base_data