{{
  config(
    materialized = 'table',
    cluster_by = ['market_code']
  )
}}

{%- set forecast_generation_date_query -%}
  select 
    (date_trunc('month', max(forecast_generation_month_date)) - interval '1 month')::date as prev_date
  from {{ ref('depletions_forecast_init_draft_chains') }}
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
  vip_parent_chain_code as parent_chain_code,
  vip_parent_chain_name,
  variant_size_pack_id,
  sum(case_equivalent_quantity) as sum_case_equivalent_depletions
from {{ ref('rad_invoice_level_sales') }}
where month_date >= date_trunc('month', current_date) - INTERVAL '12 MONTH'
  and month_date < date_trunc('month', current_date)
group by all
),

-- Build full forecast universe (all distributors per market/chain/VSP)
forecast_universe_by_market as (
  select distinct
    fc.market_code,
    fc.market_name,
    fc.distributor_id,
    fc.distributor_name,
    left(fc.distributor_id, 5) as customer_id,
    fc.parent_chain_code,
    fc.parent_chain_name,
    fc.variant_size_pack_id
  from {{ ref('depletions_forecast_init_draft_chains') }} fc
  where fc.forecast_generation_month_date >= '{{ previous_forecast_generation_month_date }}'
    and fc.data_type in ('forecast','zero_seeded')
),

-- Left join the full universe to sales data to ensure all distributors are represented
all_distributors_with_sales as (
  select
    fu.market_name,
    fu.market_code,
    fu.customer_id,
    fu.distributor_name,
    fu.distributor_id,
    fu.parent_chain_code,
    fu.parent_chain_name,
    fu.variant_size_pack_id,
    coalesce(dmd.sum_case_equivalent_depletions, 0) as sum_case_equivalent_depletions
  from forecast_universe_by_market fu
  left join deps_by_market_distro dmd
    on fu.market_code = dmd.market_code
    and fu.distributor_id = dmd.distributor_id
    and coalesce(fu.parent_chain_code,'') = coalesce(dmd.parent_chain_code,'')
    and fu.variant_size_pack_id = dmd.variant_size_pack_id
),

-- Calculate total sales for the group to determine allocation strategy
group_sales_summary as (
  select
    *,
    sum(sum_case_equivalent_depletions) over (partition by market_code, parent_chain_code, variant_size_pack_id) as group_total_sales
  from all_distributors_with_sales
),

-- Define the value to be used for allocation based on whether the group has sales
combined_allocations as (
  select
    market_name,
    market_code,
    customer_id,
    distributor_name,
    distributor_id,
    parent_chain_code,
    parent_chain_name,
    variant_size_pack_id,
    sum_case_equivalent_depletions,
    -- If the group has sales, use the actual sales value for allocation.
    -- Otherwise, use 1.0 as a base for an equal split via ratio_to_report.
    case
      when group_total_sales > 0 then sum_case_equivalent_depletions
      else 1.0
    end as allocation_value
  from group_sales_summary
)

-- Final output with corrected allocation logic
select
  ca.market_name,
  ca.market_code,
  ca.customer_id,
  ca.distributor_name,
  ca.distributor_id,
  ca.parent_chain_code,
  ca.parent_chain_name,
  ca.variant_size_pack_id,
  ca.sum_case_equivalent_depletions,
  sum(ca.sum_case_equivalent_depletions) over (partition by ca.market_code, ca.parent_chain_code, ca.variant_size_pack_id) as ttl_case_equivalent_depletions,
  coalesce(ratio_to_report(ca.allocation_value) over (partition by ca.market_code, ca.parent_chain_code, ca.variant_size_pack_id), 0) as distributor_allocation,
  sum(ca.sum_case_equivalent_depletions) over (partition by ca.market_code, ca.parent_chain_code, ca.variant_size_pack_id, ca.customer_id) as customer_ttl_case_equivalent_depletions,
  coalesce(ratio_to_report(ca.allocation_value) over (partition by ca.market_code, ca.parent_chain_code, ca.variant_size_pack_id, ca.customer_id), 0) as customer_allocation
from combined_allocations ca
left join {{ source('master_data', 'apollo_variant_size_pack_tag') }} tagp
  on tagp.variant_size_pack_id = ca.variant_size_pack_id
where parent_chain_code is not null 
  and parent_chain_code != ''
  and coalesce(lower(tagp.is_planned), 'true') != 'false'
  and (
    tagp.market_code_exclusions is null or array_size(tagp.market_code_exclusions) = 0
    or not array_contains(ca.market_code::variant, tagp.market_code_exclusions)
  )
  and (
    tagp.customer_id_exclusions is null or array_size(tagp.customer_id_exclusions) = 0
    or not array_contains(ca.customer_id::variant, tagp.customer_id_exclusions)
  )