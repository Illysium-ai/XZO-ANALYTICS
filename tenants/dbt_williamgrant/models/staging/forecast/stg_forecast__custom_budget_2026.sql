{{
  config(
    materialized = 'view'
  )
}}

select
    goals.market_code,
    asm.variant_size_pack_id,
    goals.year,
    goals.month,
    sum(goals.budget) as case_equivalent_volume_goal_stg
from {{ source('forecast', 'depletions_budget_custom_goals_2026_upload') }} goals
left join {{ ref('apollo_sku_master') }} asm
    on goals.sku_id = asm.sku_id
where try_cast(goals.budget as decimal) is not null
and asm.sku_id is not null
group by all