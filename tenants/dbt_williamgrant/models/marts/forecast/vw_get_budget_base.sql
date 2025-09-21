{{
  config(
    materialized = 'view'
  )
}}

with base as (
  select * from {{ source('forecast', 'depletions_budget_generated') }}
),
manual as (
  select * from {{ source('forecast', 'manual_input_depletions_budget') }}
),
custom_budget_2026 as (
  select * from {{ ref('stg_forecast__custom_budget_2026') }}
),
apollo_dist_master as (
  select * from {{ ref('apollo_dist_master') }}
),
apollo_variant_size_pack_tag as (
  select * from {{ ref('apollo_variant_size_pack_tag') }}
),
-- Calculate distributor allocation for custom budget distribution
distributor_allocation as (
  select 
    cb.market_code,
    cb.variant_size_pack_id,
    cb.year,
    cb.month,
    cb.case_equivalent_volume_goal_stg,
    b.distributor_id,
    b.forecast_method,
    cb.case_equivalent_volume_goal_stg / count(distinct b.distributor_id) over (
      partition by cb.market_code, cb.variant_size_pack_id, cb.year, cb.month
    ) as allocated_case_equivalent_volume
  from custom_budget_2026 cb
  inner join base b
    on cb.market_code = b.market_code
    and cb.variant_size_pack_id = b.variant_size_pack_id
    and cb.year = b.forecast_year
    and cb.month = b.month
)
select
  b.market_name,
  b.market_code,
  adm.area_name as market_area_name,
  b.distributor_name,
  b.distributor_id,
  b.brand,
  b.brand_id,
  b.variant,
  b.variant_id,
  b.variant_size_pack_desc,
  b.variant_size_pack_id,
  b.forecast_year,
  b.month,
  b.forecast_month_date,
  b.forecast_method,
  coalesce(m.manual_case_equivalent_volume, b.case_equivalent_volume) as case_equivalent_volume,
  da.allocated_case_equivalent_volume as case_equivalent_volume_goal,
  b.py_case_equivalent_volume,
  b.cy_3m_sum,
  b.cy_6m_sum,
  b.cy_12m_sum,
  b.py_3m_sum,
  b.py_6m_sum,
  b.py_12m_sum,
  b.data_source,
  b.budget_cycle_date,
  case when m.manual_case_equivalent_volume is not null then true else false end as is_manual_input,
  vst.tag_ids,
  vst.tag_names,
  m.comment
from base b
left join manual m
  on m.market_code = b.market_code
 and m.distributor_id = b.distributor_id
 and m.variant_size_pack_id = b.variant_size_pack_id
 and m.forecast_year = b.forecast_year
 and m.month = b.month
 and m.budget_cycle_date = b.budget_cycle_date
left join distributor_allocation da
  on da.market_code = b.market_code
  and da.distributor_id = b.distributor_id
  and da.variant_size_pack_id = b.variant_size_pack_id
  and da.year = b.forecast_year
  and da.month = b.month
  and da.forecast_method = b.forecast_method
left join apollo_dist_master adm
  on adm.distributor_id = b.distributor_id
left join apollo_variant_size_pack_tag vst
  on vst.variant_size_pack_id = b.variant_size_pack_id