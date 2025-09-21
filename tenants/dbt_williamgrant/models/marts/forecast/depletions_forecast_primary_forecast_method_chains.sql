{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'sync_all_columns',
    full_refresh = false,
    transient = false
  )
}}

with chain_level_method as (
  select
    s.market_code,
    s.parent_chain_code,
    s.variant_size_pack_id,
    s.forecast_method
  from (
    select
      fcm.market_code,
      fcm.parent_chain_code,
      fcm.variant_size_pack_id,
      fcm.forecast_method,
      min(fcm.distributor_id) as first_dist_id
    from {{ this }} fcm
    group by 1, 2, 3, 4
  ) s
  qualify row_number() over (
    partition by s.market_code, s.parent_chain_code, s.variant_size_pack_id
    order by s.first_dist_id
  ) = 1
),
forecast_data_with_potential_method as (
  select distinct
    fci.market_name,
    fci.market_code,
    fci.distributor_id,
    fci.parent_chain_code,
    fci.variant_size_pack_desc,
    fci.variant_size_pack_id,
    case when fci.data_source = 'previous_consensus' then 'run_rate' else coalesce(clm.forecast_method, 'six_month') end as potential_forecast_method
  from {{ ref('depletions_forecast_init_draft_chains') }} fci
  left join chain_level_method clm
    on fci.market_code = clm.market_code
    and fci.parent_chain_code = clm.parent_chain_code
    and fci.variant_size_pack_id = clm.variant_size_pack_id
  where true
  and fci.is_current_forecast_generation = 1
), primary_forecast_stage as (
  select
    fd.market_name,
    fd.market_code,
    fd.distributor_id,
    fd.parent_chain_code,
    fd.variant_size_pack_desc,
    fd.variant_size_pack_id,
    fd.potential_forecast_method as forecast_method,
    1 as is_primary_forecast_method
  from forecast_data_with_potential_method fd
  {% if is_incremental() %}
  where not exists (
    select 1
    from {{ this }} existing
    where fd.market_code = existing.market_code
      and fd.distributor_id = existing.distributor_id
      and fd.parent_chain_code = existing.parent_chain_code
      and fd.variant_size_pack_id = existing.variant_size_pack_id
  )
  {% endif %}
)

select * from primary_forecast_stage
