{{
  config(
    materialized = 'table',
    cluster_by = ['market_code']
  )
}}

{%- set forecast_generation_date_query -%}
  select 
    (date_trunc('month', max(forecast_generation_month_date)) - interval '1 month')::date as prev_date
  from {{ ref('depletions_forecast_init_draft') }}
{%- endset -%}

{%- if execute -%}
  {%- set results = run_query(forecast_generation_date_query) -%}
  {%- set previous_forecast_generation_month_date = results.columns[0].values()[0] -%}
{%- endif -%}

with deps_by_market_distro as (
select
  market_name,
  market_code,
  left(distributor_id,5) as customer_id,
  distributor_name,
  distributor_id,
  variant_size_pack_id,
  sum(case_equivalent_quantity) as sum_case_equivalent_depletions
from {{ ref('rad_distributor_level_sales') }}
where month_date >= date_trunc('month', current_date) - INTERVAL '12 MONTH'
  and month_date < date_trunc('month', current_date)
group by 1,2,3,4,5,6
),

-- Build full forecast universe (all distributors per market/VSP)
forecast_universe_by_market as (
  select distinct
    fc.market_code,
    fc.market_name,
    fc.distributor_id,
    fc.distributor_name,
    left(fc.distributor_id, 5) as customer_id,
    fc.variant_size_pack_id
  from {{ ref('depletions_forecast_init_draft') }} fc
  where fc.forecast_generation_month_date >= '{{ previous_forecast_generation_month_date }}'
    and fc.data_type in ('forecast','zero_seeded')
),

-- Identify distributor/VSP combos missing from sales
missing_distributor_variants as (
  select fu.*
  from forecast_universe_by_market fu
  left join deps_by_market_distro dmd
    on dmd.market_code = fu.market_code
    and dmd.distributor_id = fu.distributor_id
    and dmd.variant_size_pack_id = fu.variant_size_pack_id
  where dmd.distributor_id is null
),

-- Count total distributors per market/VSP in the forecast universe
forecast_counts as (
  select
    market_code,
    variant_size_pack_id,
    count(distinct distributor_id) as distributor_count
  from forecast_universe_by_market
  group by 1,2
),

-- Create equal-percentage allocation records only for the missing distributor/VSP combos
distributed_allocations as (
  select
    md.market_name,
    md.market_code,
    md.customer_id,
    md.distributor_name,
    md.distributor_id,
    md.variant_size_pack_id,
    1.0 / fc.distributor_count as sum_case_equivalent_depletions
  from missing_distributor_variants md
  join forecast_counts fc
    on fc.market_code = md.market_code
    and fc.variant_size_pack_id = md.variant_size_pack_id
),

-- Combine original allocation data with distributed gap-filling data
combined_allocations as (
  -- Original allocation data
  select * from deps_by_market_distro
  
  union all
  
  -- Distributed gap-filling data
  select * from distributed_allocations
)

-- Final output with allocation percentages (same logic as before, now using combined data)
select
  ca.*,
  sum(sum_case_equivalent_depletions) over (partition by ca.market_code, ca.variant_size_pack_id) as ttl_case_equivalent_depletions,
  coalesce(ratio_to_report(sum_case_equivalent_depletions) over (partition by ca.market_code, ca.variant_size_pack_id), 0) as distributor_allocation,
  sum(sum_case_equivalent_depletions) over (partition by ca.market_code, ca.variant_size_pack_id, ca.customer_id) as customer_ttl_case_equivalent_depletions,
  coalesce(ratio_to_report(sum_case_equivalent_depletions) over (partition by ca.market_code, ca.variant_size_pack_id, ca.customer_id), 0) as customer_allocation
from combined_allocations ca
left join {{ source('master_data', 'apollo_variant_size_pack_tag') }} tagp
  on tagp.variant_size_pack_id = ca.variant_size_pack_id
where coalesce(lower(tagp.is_planned), 'true') != 'false'
  and (
    tagp.market_code_exclusions is null or array_size(tagp.market_code_exclusions) = 0
    or not array_contains(ca.market_code::variant, tagp.market_code_exclusions)
  )
  and (
    tagp.customer_id_exclusions is null or array_size(tagp.customer_id_exclusions) = 0
    or not array_contains(ca.customer_id::variant, tagp.customer_id_exclusions)
  )