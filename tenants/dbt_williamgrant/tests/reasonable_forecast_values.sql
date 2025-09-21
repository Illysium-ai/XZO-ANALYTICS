{{ config(enabled=false) }}

-- Test that ensures forecast values are within a reasonable range
-- Checks that forecast values aren't negative and aren't unreasonably high
-- compared to historical values
-- A valid test should return zero rows

with historical_max as (
    select
        state,
        market_name,
        market_code,
        distributor_name,
        distributor_id,
        brand,
        brand_id,
        variant,
        variant_id,
        variant_size_pack_id,
        max(case_equivalent_volume) * 5 as max_reasonable_value  -- Changed to case_equivalent_volume, 5x historical max as threshold
    from {{ ref('depletions_forecast_init_draft') }} -- Updated model reference
    where data_type = 'actual_complete'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 -- Adjusted group by for removed size_pack
)

select
    f.state,
    f.market_name,
    f.market_code,
    f.distributor_name,
    f.distributor_id,
    f.brand,
    f.brand_id,
    f.variant,
    f.variant_id,
    f.variant_size_pack_id,
    f.forecast_year,
    f.month,
    f.forecast_method,
    f.case_equivalent_volume as total_depletions, -- Changed to case_equivalent_volume
    h.max_reasonable_value
from {{ ref('depletions_forecast_init_draft') }} f -- Updated model reference
join historical_max h
    on f.state = h.state
    and f.market_name = h.market_name
    and f.market_code = h.market_code
    and f.distributor_id = h.distributor_id
    and f.brand_id = h.brand_id
    and f.variant_id = h.variant_id
    and f.variant_size_pack_id = h.variant_size_pack_id -- This join key is correct
where f.data_type = 'forecast'
  and (
      f.case_equivalent_volume < 0  -- Changed to case_equivalent_volume, Negative values
      or f.case_equivalent_volume > h.max_reasonable_value  -- Changed to case_equivalent_volume, Unreasonably high values
  )