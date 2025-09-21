{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['market_code', 'distributor_id', 'variant_size_pack_id', 'forecast_generation_month_date'],
    on_schema_change = 'sync_all_columns',
    full_refresh = false,
    transient = false
  )
}}

with market_level_method as (
  select
    s.market_code,
    s.variant_size_pack_id,
    s.current_fgmd as forecast_generation_month_date,
    s.forecast_method
  from (
    select
      fcm.market_code,
      fcm.variant_size_pack_id,
      fcm.forecast_generation_month_date as current_fgmd,
      fcm.forecast_method,
      min(fcm.distributor_id) as first_dist_id
    from {{ this }} fcm
    group by 1, 2, 3, 4
  ) s
  qualify row_number() over (
    partition by s.market_code, s.variant_size_pack_id, s.current_fgmd
    order by s.first_dist_id
  ) = 1
),
forecast_data_with_potential_method as (
  select distinct
    fci.market_name,
    fci.market_code,
    fci.distributor_id,
    fci.variant_size_pack_desc,
    fci.variant_size_pack_id,
    case
      when fci.data_source = 'previous_consensus' then 'run_rate'
      else coalesce(fcm.forecast_method, mlm.forecast_method, 'six_month')
    end as potential_forecast_method,
    fci.forecast_generation_month_date
  from {{ ref('depletions_forecast_init_draft') }} fci
  left join {{ this }} fcm
    on fci.market_code = fcm.market_code
    and fci.distributor_id = fcm.distributor_id
    and fci.variant_size_pack_id = fcm.variant_size_pack_id
    and fci.forecast_generation_month_date = dateadd(month, 1, fcm.forecast_generation_month_date)
  left join market_level_method mlm
    on fci.market_code = mlm.market_code
    and fci.variant_size_pack_id = mlm.variant_size_pack_id
    and fci.forecast_generation_month_date = mlm.forecast_generation_month_date
  where true
  and fci.is_current_forecast_generation = 1
  {% if is_incremental() %}
  and fci.forecast_generation_month_date >= (select coalesce(max(forecast_generation_month_date), '1970-01-01'::DATE) from {{ this }})
  {% endif %}
), primary_forecast_stage as (
  select
    fd.market_name,
    fd.market_code,
    fd.distributor_id,
    fd.variant_size_pack_desc,
    fd.variant_size_pack_id,
    {% if is_incremental() %}
    coalesce(existing.forecast_method, fd.potential_forecast_method) as forecast_method,
    {% else %}
    fd.potential_forecast_method as forecast_method,
    {% endif %}
    1 as is_primary_forecast_method,
    fd.forecast_generation_month_date
  from forecast_data_with_potential_method fd
  {% if is_incremental() %}
  left join {{ this }} existing
    on fd.market_code = existing.market_code
    and fd.distributor_id = existing.distributor_id
    and fd.variant_size_pack_id = existing.variant_size_pack_id
    and fd.forecast_generation_month_date = existing.forecast_generation_month_date
  where existing.variant_size_pack_id is null
  {% endif %}
)

select * from primary_forecast_stage
{# where market_code is not null
  and distributor_id is not null
  and variant_size_pack_id is not null
  and forecast_generation_month_date is not null
  and forecast_method is not null #}
