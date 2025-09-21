{{
  config(
    materialized = 'view'
  )
}}

with refined as (
select
    *,
    case when warehouse >= direct_import then 'warehouse' else 'direct_import' end as gsv_rate_type,
    greatest(direct_import, warehouse) as gsv_rate
from {{ source('master_data', 'hyperion_gsv_rates') }}
)
select * from refined