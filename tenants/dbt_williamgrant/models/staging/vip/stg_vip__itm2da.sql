{{
  config(
    materialized = 'view'
  )
}}

with staged as (
  select
    itm.*,
    coalesce(itm.generic_cat1,
      trim(REGEXP_REPLACE(
        itm.desc,
        '\\\\s+\\\\d+[xX]\\\\d+(\\\\.\\\\d+)?([mM]?[lL]).*',
        ''
      ))) as variant,
    itm.ext_m_lp_case / itm.units as standardized_unit_volume_ml,
    -- Divide ML by 1000 and divide by 9 to get 9L case equivalent factor per SKU sale
    (itm.ext_m_lp_case / 1000.0 / 9.0)::float as case_equivalent_factor
  from {{ source('vip', 'itm2da') }} itm
) 

select * from staged