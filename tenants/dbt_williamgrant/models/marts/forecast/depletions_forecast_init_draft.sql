{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['forecast_generation_month_date'],
    on_schema_change = 'sync_all_columns',
    cluster_by = ['forecast_generation_month_date', 'market_code', 'forecast_method'],
    post_hook=[
      "update {{ this }} set is_current_forecast_generation = 0 where forecast_generation_month_date < (select max(forecast_generation_month_date) from {{ this }})"
    ],
    transient = false,
    full_refresh = false
  )
}}

{% set backfill_fgmd = var('backfill_fgmd', none) %}

-- Depletions Forecast Incremental Table - Calculates depletion forecasts using various methods (3m, 6m, 9m, flat)
-- Captures monthly snapshots based on latest complete month.
-- Supports backfilling via dbt run --vars '{"backfill_fgmd": "2025-07-01"}'

with source_data as (
  -- Select base data, ensure month_date is present or constructed
  select
    rad.market_name,
    rad.market_code,
    rad.distributor_name,
    rad.distributor_id,
    rad.brand,
    rad.brand_id,
    rad.variant,
    rad.variant_id,
    rad.variant_size_pack_desc,
    rad.variant_size_pack_id,
    rad.year,
    rad.month,
    rad.month_date,
    DATEADD(YEAR, 1, rad.month_date) as forecast_ref_date,
    sum(coalesce(rad.case_equivalent_quantity, 0)) as case_equivalent_depletions
  from
    {{ ref('rad_distributor_level_sales') }} rad
  {% if backfill_fgmd %}
  -- When backfilling, only include data up to the month before the backfill FGMD
  where rad.month_date <= '{{ backfill_fgmd }}'::date
  {% endif %}
  group by all
),

latest_source_info as (
  -- Determine the latest month for which any data exists
  select
    max(month_date) as max_data_month_date,
    extract(month from max(month_date))::INTEGER as forecast_generation_month,
    extract(year from max(month_date))::INTEGER as forecast_generation_year
  from source_data
),

run_details as (
  -- Determine key dates for this run based on latest source data
  select
    case
      when max_data_month_date = (DATE_TRUNC('MONTH', max_data_month_date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')
        then max_data_month_date
      else (max_data_month_date - INTERVAL '1 MONTH')
    end::DATE as latest_complete_month_date,

    case
      when max_data_month_date = (DATE_TRUNC('MONTH', max_data_month_date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')
        then (DATE_TRUNC('MONTH', max_data_month_date) + INTERVAL '1 MONTH')
      else max_data_month_date
    end::DATE as forecast_generation_month_date,

    case
      when max_data_month_date = (DATE_TRUNC('MONTH', max_data_month_date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')
        then extract(month from (DATE_TRUNC('MONTH', max_data_month_date) + INTERVAL '1 MONTH'))::INTEGER
      else forecast_generation_month
    end::INTEGER as forecast_generation_month,

    case
      when max_data_month_date = (DATE_TRUNC('MONTH', max_data_month_date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')
        then extract(year from (DATE_TRUNC('MONTH', max_data_month_date) + INTERVAL '1 MONTH'))::INTEGER
      else forecast_generation_year
    end::INTEGER as forecast_generation_year,

    case
      when max_data_month_date = (DATE_TRUNC('MONTH', max_data_month_date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')
        then DATE_FROM_PARTS(extract(year from (DATE_TRUNC('MONTH', max_data_month_date) + INTERVAL '1 MONTH'))::INTEGER, 1, 1)
      else DATE_FROM_PARTS(forecast_generation_year, 1, 1)
    end::DATE as forecast_generation_year_start_date
  from
    latest_source_info
),

-- Step 1: Get unique ID combinations
unique_ids as (
  select distinct
    market_code,
    distributor_id,
    variant_size_pack_id
  from
    source_data
  where month_date >= (select DATE_FROM_PARTS(extract(year from max(max_data_month_date))::INTEGER - 1, 1, 1) from latest_source_info)
),

-- Step 2: Get the latest descriptive fields for these unique IDs
unique_product_combinations_with_descriptives as (
  select
    src.market_code,
    src.distributor_id,
    src.variant_size_pack_id,
    src.market_name,
    src.distributor_name,
    src.brand,
    src.brand_id,
    src.variant,
    src.variant_id,
    src.variant_size_pack_desc
  from
    source_data src
  inner join unique_ids ui
    on src.market_code = ui.market_code
    and src.distributor_id = ui.distributor_id
    and src.variant_size_pack_id = ui.variant_size_pack_id
  where src.month_date >= (select DATE_FROM_PARTS(extract(year from max(max_data_month_date))::INTEGER - 1, 1, 1) from latest_source_info)
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

all_months_for_densification AS (
  SELECT
    DATEADD(MONTH, (n.seq_num - 1), DATE_TRUNC('YEAR', rd.latest_complete_month_date))::DATE AS month_date
  FROM
    run_details rd,
    (SELECT SEQ4() AS seq_num FROM TABLE(GENERATOR(ROWCOUNT => 12))) n
  WHERE DATEADD(MONTH, (n.seq_num - 1), DATE_TRUNC('YEAR', rd.latest_complete_month_date)) <= rd.latest_complete_month_date
),

densified_data as (
  select
    upc.market_name,
    upc.market_code,
    upc.distributor_name,
    upc.distributor_id,
    upc.brand,
    upc.brand_id,
    upc.variant,
    upc.variant_id,
    upc.variant_size_pack_desc,
    upc.variant_size_pack_id,
    amfd.month_date,
    EXTRACT(YEAR FROM amfd.month_date)::INTEGER AS year,
    EXTRACT(MONTH FROM amfd.month_date)::INTEGER AS month,
    DATEADD(YEAR, 1, amfd.month_date) AS forecast_ref_date,
    COALESCE(sd.case_equivalent_depletions, 0.0) AS case_equivalent_depletions
  from
    unique_product_combinations_with_descriptives upc
  cross join
    all_months_for_densification amfd
  left join
    source_data sd
    on upc.market_code = sd.market_code
    and upc.distributor_id = sd.distributor_id
    and upc.variant_size_pack_id = sd.variant_size_pack_id
    and amfd.month_date = sd.month_date
  union all
  select
    sd.market_name,
    sd.market_code,
    sd.distributor_name,
    sd.distributor_id,
    sd.brand,
    sd.brand_id,
    sd.variant,
    sd.variant_id,
    sd.variant_size_pack_desc,
    sd.variant_size_pack_id,
    sd.month_date,
    sd.year,
    sd.month,
    sd.forecast_ref_date,
    sd.case_equivalent_depletions
  from
    source_data sd
  where
    NOT EXISTS (
      SELECT 1
      FROM all_months_for_densification amfd_check
      WHERE sd.month_date = amfd_check.month_date
    )
),

monthly_data_with_py as (
  select
    dd.*,
    coalesce(py.case_equivalent_depletions, 0.0) as py_case_equivalent_depletions
  from
    densified_data dd
  left join
    source_data py
    on dd.market_code = py.market_code
    and dd.distributor_id = py.distributor_id
    and dd.variant_size_pack_id = py.variant_size_pack_id
    and dd.month_date = py.forecast_ref_date
),

rolling_sums as (
  select
    md.*,
    rd.latest_complete_month_date,
    sum(case_equivalent_depletions) over (
      partition by market_code, distributor_id, variant_size_pack_id
      order by month_date
      RANGE BETWEEN INTERVAL '2 months' PRECEDING AND CURRENT ROW
    ) as cy_3m_sum,
    sum(case_equivalent_depletions) over (
      partition by market_code, distributor_id, variant_size_pack_id
      order by month_date
      RANGE BETWEEN INTERVAL '5 months' PRECEDING AND CURRENT ROW
    ) as cy_6m_sum,
    sum(case_equivalent_depletions) over (
      partition by market_code, distributor_id, variant_size_pack_id
      order by month_date
      RANGE BETWEEN INTERVAL '11 months' PRECEDING AND CURRENT ROW
    ) as cy_12m_sum,
    sum(py_case_equivalent_depletions) over (
      partition by market_code, distributor_id, variant_size_pack_id
      order by month_date
      RANGE BETWEEN INTERVAL '2 months' PRECEDING AND CURRENT ROW
    ) as py_3m_sum,
    sum(py_case_equivalent_depletions) over (
      partition by market_code, distributor_id, variant_size_pack_id
      order by month_date
      RANGE BETWEEN INTERVAL '5 months' PRECEDING AND CURRENT ROW
    ) as py_6m_sum,
    sum(py_case_equivalent_depletions) over (
      partition by market_code, distributor_id, variant_size_pack_id
      order by month_date
      RANGE BETWEEN INTERVAL '11 months' PRECEDING AND CURRENT ROW
    ) as py_12m_sum
  from
    monthly_data_with_py md
  cross join run_details rd
),

-- FGMD-only month-end projection at distributor grain
fgmd_prediction as (
  select
    p.market_code,
    p.distributor_id,
    p.variant_size_pack_id,
    p.month_date as forecast_month_date,
    p.projected_case_equivalent_quantity,
    p.projection_method_used
  from {{ forecast__monthend_projection('distributor', 'fgmd') }} p
),

trend_factors_calculated as (
  select
    market_name,
    market_code,
    distributor_name,
    distributor_id,
    brand,
    brand_id,
    variant,
    variant_id,
    variant_size_pack_desc,
    variant_size_pack_id,
    latest_complete_month_date,
    -- Calculate Run Rate as the average month in the last 3 months per Feb SnOP Tool file.
    cy_3m_sum / 3 as run_rate_3m,
    -- Calculate ratios, defaulting to 1.0 if PY sum is 0
    -- Max of 1.5 to prevent extreme outliers
    least(coalesce(cy_3m_sum / nullif(py_3m_sum, 0), 1.0), 1.5) as trend_factor_3m,
    least(coalesce(cy_6m_sum / nullif(py_6m_sum, 0), 1.0), 1.5) as trend_factor_6m,
    least(coalesce(cy_12m_sum / nullif(py_12m_sum, 0), 1.0), 1.5) as trend_factor_12m
  from
    rolling_sums
  where
    month_date = latest_complete_month_date -- Only need factors calculated at the reference point
),

trend_factors_unpivoted as (
  -- Unpivot the calculated factors and add the 'flat' method
  select
    market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id,
    latest_complete_month_date,
    'three_month' as forecast_method, trend_factor_3m as trend_factor
  from trend_factors_calculated
  union all
  select
    market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id,
    latest_complete_month_date,
    'six_month' as forecast_method, trend_factor_6m as trend_factor
  from trend_factors_calculated
  union all
  select
    market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id,
    latest_complete_month_date,
    'twelve_month' as forecast_method, trend_factor_12m as trend_factor
  from trend_factors_calculated
  union all
  select
    market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id,
    latest_complete_month_date,
    'flat' as forecast_method, 1.0 as trend_factor
  from trend_factors_calculated
  union all
  select
    market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id,
    latest_complete_month_date,
    'run_rate' as forecast_method, run_rate_3m as trend_factor
  from trend_factors_calculated
),

future_months_template as (
  -- Generate 12 months starting from the month AFTER the latest complete month
  -- Forecast only the next 12 months so PY actuals are used and forecasts do not overlap
  select
    DATEADD(MONTH, m.month_num, rd.latest_complete_month_date)::DATE as forecast_month_date,
    extract(year from DATEADD(MONTH, m.month_num, rd.latest_complete_month_date))::INTEGER as forecast_year,
    extract(month from DATEADD(MONTH, m.month_num, rd.latest_complete_month_date))::INTEGER as month
  from
    (select SEQ4() as month_num from TABLE(GENERATOR(ROWCOUNT => 13))) m
  cross join
    run_details rd
  where m.month_num >= 1  -- Start from month 1 (the month AFTER latest_complete_month_date)
),

current_year_actuals as (
    select
        sd.market_code,
        sd.distributor_id,
        sd.variant_size_pack_id,
        sd.month_date,
        sd.year,
        sd.month,
        sd.case_equivalent_depletions as actual_case_equivalent_depletions,
        sd.market_name,
        sd.distributor_name,
        sd.brand,
        sd.brand_id,
        sd.variant,
        sd.variant_id,
        sd.variant_size_pack_desc
    from
        densified_data sd
    join run_details rd on sd.month_date <= rd.latest_complete_month_date
                     and sd.year = rd.forecast_generation_year
),

distinct_methods as (
    select 'three_month' as forecast_method
    union all
    select 'six_month' as forecast_method
    union all
    select 'twelve_month' as forecast_method
    union all
    select 'flat' as forecast_method
    union all
    select 'run_rate' as forecast_method
),

-- First, forecast months 1-12 which can use actual PY data
forecast_rows as (
    select
        tfu.market_name,
        tfu.market_code,
        tfu.distributor_name,
        tfu.distributor_id,
        tfu.brand,
        tfu.brand_id,
        tfu.variant,
        tfu.variant_id,
        tfu.variant_size_pack_desc,
        tfu.variant_size_pack_id,
        fmt.forecast_year,
        fmt.month,
        fmt.forecast_month_date,
        tfu.forecast_method,
        greatest(
          case
            when fmt.forecast_month_date = rd.forecast_generation_month_date and coalesce(adm_for_area.area_name, '') not in ('Control')
              then coalesce(fp.projected_case_equivalent_quantity, 0.0)
            when fmt.forecast_month_date = rd.forecast_generation_month_date and coalesce(adm_for_area.area_name, '') in ('Control') and fp.projected_case_equivalent_quantity is not null
              then fp.projected_case_equivalent_quantity
            when tfu.forecast_method = 'run_rate' then coalesce(tfu.trend_factor, 1.0)
            else coalesce(pya.case_equivalent_depletions, 0.0) * coalesce(tfu.trend_factor, 1.0)
          end,
          0.0) as case_equivalent_volume,
        coalesce(pya.case_equivalent_depletions, 0.0) as py_case_equivalent_volume,
        'forecast' as data_type,
        coalesce(tfu.trend_factor, 1.0) as trend_factor,
        rd.latest_complete_month_date,
        rd.forecast_generation_month_date,
        rd.forecast_generation_year,
        'draft' as forecast_status,
        current_timestamp as updated_at,
        rs.cy_3m_sum,
        rs.cy_6m_sum,
        rs.cy_12m_sum,
        rs.py_3m_sum,
        rs.py_6m_sum,
        rs.py_12m_sum
    from
        trend_factors_unpivoted tfu
    cross join
        future_months_template fmt
    cross join run_details rd
    left join
        source_data pya on tfu.market_code = pya.market_code
            and tfu.distributor_id = pya.distributor_id
            and tfu.variant_size_pack_id = pya.variant_size_pack_id
            and fmt.forecast_month_date = pya.forecast_ref_date
    left join
        rolling_sums rs on tfu.market_code = rs.market_code
            and tfu.distributor_id = rs.distributor_id
            and tfu.variant_size_pack_id = rs.variant_size_pack_id
            and rs.month_date = rd.latest_complete_month_date
    left join fgmd_prediction fp
        on fp.market_code = tfu.market_code
       and fp.distributor_id = tfu.distributor_id
       and fp.variant_size_pack_id = tfu.variant_size_pack_id
       and fp.forecast_month_date = fmt.forecast_month_date
    left join {{ ref('apollo_dist_master') }} adm_for_area
        on adm_for_area.distributor_id = tfu.distributor_id
    where
        -- Only first 12 months (these can use actual PY data)
        DATEDIFF(MONTH, rd.latest_complete_month_date, fmt.forecast_month_date) <= 12
),

actual_rows as (
    select
        cya.market_name,
        cya.market_code,
        cya.distributor_name,
        cya.distributor_id,
        cya.brand,
        cya.brand_id,
        cya.variant,
        cya.variant_id,
        cya.variant_size_pack_desc,
        cya.variant_size_pack_id,
        cya.year as forecast_year,
        cya.month,
        cya.month_date as forecast_month_date,
        dm.forecast_method,
        cya.actual_case_equivalent_depletions as case_equivalent_volume,
        pya.case_equivalent_depletions as py_case_equivalent_volume,
        'actual_complete' as data_type,
        NULL::FLOAT as trend_factor,
        rd.latest_complete_month_date,
        rd.forecast_generation_month_date,
        rd.forecast_generation_year,
        'draft' as forecast_status,
        current_timestamp as updated_at,
        rs.cy_3m_sum,
        rs.cy_6m_sum,
        rs.cy_12m_sum,
        rs.py_3m_sum,
        rs.py_6m_sum,
        rs.py_12m_sum
    from
        current_year_actuals cya
    cross join distinct_methods dm -- Create a row for each actual month per method
    cross join run_details rd
    left join
        source_data pya on cya.market_code = pya.market_code
            and cya.distributor_id = pya.distributor_id
            and cya.variant_size_pack_id = pya.variant_size_pack_id
            and cya.month_date = pya.forecast_ref_date
    left join
        rolling_sums rs on cya.market_code = rs.market_code
            and cya.distributor_id = rs.distributor_id
            and cya.variant_size_pack_id = rs.variant_size_pack_id
            and rs.month_date = rd.latest_complete_month_date
),

combined_data as (
    select * from actual_rows
    union all
    select * from forecast_rows
),

asm_vsp as (
  select distinct
    variant_size_pack_id,
    variant_size_pack_desc,
    brand,
    brand_id,
    variant,
    variant_id
  from {{ ref('apollo_sku_master') }}
),

planned_zero_rows as (
  -- Zero rows for planned, non-excluded products across all markets/distributors for next 12 months and all methods,
  -- only when no rows exist in combined_data for the same key
  select
    adm.market_name,
    adm.market_code,
    adm.distributor_name,
    adm.distributor_id,
    av.brand,
    av.brand_id,
    av.variant,
    av.variant_id,
    tag.variant_size_pack_desc,
    tag.variant_size_pack_id,
    fmt.forecast_year,
    fmt.month,
    fmt.forecast_month_date,
    mth.forecast_method,
    0.0::float as case_equivalent_volume,
    0.0::float as py_case_equivalent_volume,
    'forecast' as data_type,
    1.0::float as trend_factor,
    rd.latest_complete_month_date,
    rd.forecast_generation_month_date,
    rd.forecast_generation_year,
    'draft' as forecast_status,
    current_timestamp as updated_at,
    cast(null as float) as cy_3m_sum,
    cast(null as float) as cy_6m_sum,
    cast(null as float) as cy_12m_sum,
    cast(null as float) as py_3m_sum,
    cast(null as float) as py_6m_sum,
    cast(null as float) as py_12m_sum
  from {{ source('master_data', 'apollo_variant_size_pack_tag') }} tag
  cross join future_months_template fmt
  cross join run_details rd
  join {{ ref('apollo_dist_master') }} adm on 1=1
  left join asm_vsp av
    on av.variant_size_pack_id = tag.variant_size_pack_id
  cross join (
    select 'three_month' as forecast_method
    union all select 'six_month'
    union all select 'twelve_month'
    union all select 'flat'
    union all select 'run_rate'
  ) mth
  where lower(tag.is_planned) = 'true'
    and adm.is_depletions_eligible = 1
    -- Note: NULL is_planned leads to no zero seeding; products appear only if actuals exist
    and (
      tag.market_code_exclusions is null or array_size(tag.market_code_exclusions) = 0
      or not array_contains(adm.market_code::variant, tag.market_code_exclusions)
    )
    and (
      tag.customer_id_exclusions is null or array_size(tag.customer_id_exclusions) = 0
      or not array_contains(adm.distributor_id::variant, tag.customer_id_exclusions)
    )
    and not exists (
      select 1
      from combined_data cd
      where cd.market_code = adm.market_code
        and cd.distributor_id = adm.distributor_id
        and cd.variant_size_pack_id = tag.variant_size_pack_id
        and cd.forecast_generation_month_date = rd.forecast_generation_month_date
    )
    and datediff(month, rd.latest_complete_month_date, fmt.forecast_month_date) <= 12
),

-- NEW: Integrated data combining logic-driven and planned zero rows
integrated_data as (
  -- Logic-driven forecasts
  select 
    market_name,
    market_code,
    distributor_name,
    distributor_id,
    brand,
    brand_id,
    variant,
    variant_id,
    variant_size_pack_desc,
    variant_size_pack_id,
    forecast_year,
    month,
    forecast_month_date,
    forecast_method,
    case_equivalent_volume,
    py_case_equivalent_volume,
    data_type,
    trend_factor,
    latest_complete_month_date,
    forecast_generation_month_date,
    forecast_generation_year,
    cy_3m_sum,
    cy_6m_sum,
    cy_12m_sum,
    py_3m_sum,
    py_6m_sum,
    py_12m_sum,
    1 as is_current_forecast_generation,
    forecast_status,
    updated_at,
    'logic_driven' as data_source
  from combined_data
  union all
  -- Planned zero rows
  select 
    market_name,
    market_code,
    distributor_name,
    distributor_id,
    brand,
    brand_id,
    variant,
    variant_id,
    variant_size_pack_desc,
    variant_size_pack_id,
    forecast_year,
    month,
    forecast_month_date,
    forecast_method,
    case_equivalent_volume,
    py_case_equivalent_volume,
    data_type,
    trend_factor,
    latest_complete_month_date,
    forecast_generation_month_date,
    forecast_generation_year,
    cy_3m_sum,
    cy_6m_sum,
    cy_12m_sum,
    py_3m_sum,
    py_6m_sum,
    py_12m_sum,
    1 as is_current_forecast_generation,
    'draft' as forecast_status,
    current_timestamp as updated_at,
    'zero_seeded' as data_source
  from planned_zero_rows
)

-- final select from integrated data (was combined_data)
select
  id.market_name,
  id.market_code,
  adm.area_name AS market_area_name,
  adm.customer_id,
  adm.customer_name,
  id.distributor_name,
  id.distributor_id,
  id.brand,
  id.brand_id,
  id.variant,
  id.variant_id,
  id.variant_size_pack_desc,
  id.variant_size_pack_id,
  id.forecast_year,
  id.month,
  id.forecast_month_date,
  id.forecast_method,
  -- id.default_forecast_method,
  id.case_equivalent_volume,
  id.py_case_equivalent_volume,
  id.data_type,
  id.trend_factor,
  id.latest_complete_month_date,
  id.forecast_generation_month_date,
  id.forecast_generation_year,
  id.cy_3m_sum as cy_3m_case_equivalent_volume,
  id.cy_6m_sum as cy_6m_case_equivalent_volume,
  id.cy_12m_sum as cy_12m_case_equivalent_volume,
  id.py_3m_sum as py_3m_case_equivalent_volume,
  id.py_6m_sum as py_6m_case_equivalent_volume,
  id.py_12m_sum as py_12m_case_equivalent_volume,
  -- Add flag for current forecast generation
  id.is_current_forecast_generation,
  id.forecast_status,
  id.updated_at,
  id.data_source
from integrated_data id
left join {{ ref('apollo_dist_master') }} adm
  on id.distributor_id = adm.distributor_id
left join {{ source('master_data', 'apollo_variant_size_pack_tag') }} tagp
  on tagp.variant_size_pack_id = id.variant_size_pack_id
where coalesce(lower(tagp.is_planned), 'true') != 'false'
  and (
    tagp.market_code_exclusions is null or array_size(tagp.market_code_exclusions) = 0
    or not array_contains(id.market_code::variant, tagp.market_code_exclusions)
  )
  and (
    tagp.customer_id_exclusions is null or array_size(tagp.customer_id_exclusions) = 0
    or not array_contains(id.distributor_id::variant, tagp.customer_id_exclusions)
  )

{% if is_incremental() %}
  and (
  {% if backfill_fgmd %}
  -- Backfill mode: Only process the specific FGMD
  id.forecast_generation_month_date = '{{ backfill_fgmd }}'::date
  {% else %}
  -- Normal mode: Include rows if they belong to a new forecast generation month
  -- (works for both logic_driven and previous_consensus data sources)
  id.forecast_generation_month_date >= (select coalesce(max(forecast_generation_month_date), '1979-01-01'::date) from {{ this }})
  {% endif %}
  )
{% endif %}

order by
    id.market_code,
    id.distributor_id,
    id.variant_size_pack_id,
    id.forecast_year,
    id.month,
    id.forecast_method,
    id.forecast_generation_month_date