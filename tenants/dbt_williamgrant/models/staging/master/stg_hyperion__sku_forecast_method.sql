{{
  config(
    materialized = 'view'
  )
}}

with refined as (
select distinct
  asm.variant_size_pack_id,
  case
    when lower(fc.forecast_method) IN ('disco','run rate','benchmark','custom') then 'run_rate'
    when lower(fc.forecast_method) = '12mo' then 'twelve_month'
    when lower(fc.forecast_method) = '6mo' then 'six_month'
    when lower(fc.forecast_method) = '3mo' then 'three_month'
    when lower(fc.forecast_method) = 'flat' then 'flat'
    end as primary_forecast_method,
  case
    when lower(fc.forecast_method) IN ('disco','run rate','benchmark','custom') then 1
    when lower(fc.forecast_method) = '12mo' then 2
    when lower(fc.forecast_method) = '6mo' then 3
    when lower(fc.forecast_method) = '3mo' then 4
    when lower(fc.forecast_method) = 'flat' then 5
    end as primary_forecast_method_rank
from {{ source('master_data', 'hyperion_sku_forecast_method') }} fc
inner join {{ ref('apollo_sku_master') }} asm
  on fc.hyperion_sku = asm.hp_size_pack
)
, ranked as (
  select
    *,
    row_number() over (partition by variant_size_pack_id order by primary_forecast_method_rank) as rn
  from refined
)
select
  *
from ranked
where rn = 1