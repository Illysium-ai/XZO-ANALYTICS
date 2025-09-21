{{
  config(
    materialized = 'incremental',
    unique_key = ['market_code', 'customer_id', 'variant_size_pack_id'],
    on_schema_change = 'sync_all_columns'
  )
}}

select
    -- gsv.market as hp_market,
    adist.market_code as market_code,
    -- gsv.planning_member,
    adist.customer_id,
    -- gsv.size_pack as hp_size_pack,
    asku.variant_size_pack_id,
    max(gsv.gsv_rate::float) as gsv_rate
from {{ ref('stg_hyperion__gsv_rates') }} gsv
left join {{ ref('apollo_dist_master') }} adist
    on gsv.market = adist.hp_market
    and gsv.planning_member = adist.hp_planning_member
left join {{ ref('apollo_sku_master') }} asku
    on gsv.size_pack = asku.hp_size_pack
where adist.market_code != '0001'
group by adist.market_code, adist.customer_id, asku.variant_size_pack_id