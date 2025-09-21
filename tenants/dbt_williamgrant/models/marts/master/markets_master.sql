{{
  config(
    materialized = 'view'
  )
}}

with dist_markets as (
  select
    market_code,
    listagg(distinct area_code, ' | ') as area_code,
    listagg(distinct area_name, ' | ') as area_description
  from {{ ref('apollo_dist_master') }}
  where is_depletions_eligible = 1
  group by all
)
SELECT
    ms.id,
    ms.market_name,
    ms.market_code,
    mm.market_hyperion,
    mm.market_coding,
    mm.market_id,
    dm.area_code,
    dm.area_description,
    mm.customers,
    ms.settings
FROM {{ source('master_data', 'market_settings') }} ms
LEFT JOIN {{ ref('markets_hyperion') }} mm ON ms.market_code = mm.market_code
LEFT JOIN dist_markets dm ON mm.market_id = dm.market_code
ORDER BY ms.id