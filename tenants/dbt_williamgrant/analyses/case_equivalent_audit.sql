-- Case Equivalent Factor Audit
-- This analysis helps validate that case equivalent factors are correctly calculated
-- in the production environment for William Grant's spirits products

with parsed_sizes as (
  select
    sku_id,
    sku_description,
    size_pack,
    case_equivalent_factor,
    
    -- Extract bottle count if available in size_pack format (e.g., '12x750ml')
    case
      when regexp_like(size_pack, '\\d+x') 
      then cast(regexp_substr(size_pack, '\\d+(?=x)') as decimal(10,2))
      else null
    end as extracted_bottle_count,
    
    -- Extract bottle size in ml if available
    case
      when regexp_like(size_pack, '(?<=x)\\d+(?=ml)') 
      then cast(regexp_substr(size_pack, '(?<=x)\\d+(?=ml)') as decimal(10,2))
      when regexp_like(size_pack, '(?<=x)\\d+\\.?\\d*(?=L)') 
      then cast(regexp_substr(size_pack, '(?<=x)\\d+\\.?\\d*(?=L)') as decimal(10,2)) * 1000
      else null
    end as extracted_ml_per_bottle,
    
    -- Calculate total volume in liters
    case
      when regexp_like(size_pack, '\\d+x') and regexp_like(size_pack, '(?<=x)\\d+(?=ml)') 
      then cast(regexp_substr(size_pack, '\\d+(?=x)') as decimal(10,2)) * 
           cast(regexp_substr(size_pack, '(?<=x)\\d+(?=ml)') as decimal(10,2)) / 1000
      when regexp_like(size_pack, '\\d+x') and regexp_like(size_pack, '(?<=x)\\d+\\.?\\d*(?=L)') 
      then cast(regexp_substr(size_pack, '\\d+(?=x)') as decimal(10,2)) * 
           cast(regexp_substr(size_pack, '(?<=x)\\d+\\.?\\d*(?=L)') as decimal(10,2))
      else null
    end as calculated_total_liters,
    
    -- Calculate expected case equivalent factor (total liters / 9)
    case
      when regexp_like(size_pack, '\\d+x') and regexp_like(size_pack, '(?<=x)\\d+(?=ml)') 
      then (cast(regexp_substr(size_pack, '\\d+(?=x)') as decimal(10,2)) * 
            cast(regexp_substr(size_pack, '(?<=x)\\d+(?=ml)') as decimal(10,2)) / 1000) / 9
      when regexp_like(size_pack, '\\d+x') and regexp_like(size_pack, '(?<=x)\\d+\\.?\\d*(?=L)') 
      then (cast(regexp_substr(size_pack, '\\d+(?=x)') as decimal(10,2)) * 
            cast(regexp_substr(size_pack, '(?<=x)\\d+\\.?\\d*(?=L)') as decimal(10,2))) / 9
      else null
    end as expected_case_equivalent
    
  from {{ ref('stg_product') }}
  where case_equivalent_factor is not null
)

-- Output analysis results
select
  sku_id,
  sku_description,
  size_pack,
  extracted_bottle_count as bottles,
  extracted_ml_per_bottle as ml_per_bottle,
  calculated_total_liters as total_liters,
  case_equivalent_factor as actual_factor,
  expected_case_equivalent as expected_factor,
  case 
    when expected_case_equivalent is null then null
    else abs(case_equivalent_factor - expected_case_equivalent) 
  end as difference,
  case
    when expected_case_equivalent is null then 'Unparseable format'
    when abs(case_equivalent_factor - expected_case_equivalent) <= 0.01 then 'Correct'
    else 'Discrepancy'
  end as validation_status
from parsed_sizes
order by 
  case
    when expected_case_equivalent is null then 2
    when abs(case_equivalent_factor - expected_case_equivalent) <= 0.01 then 0
    else 1
  end,
  abs(case_equivalent_factor - expected_case_equivalent) desc;
