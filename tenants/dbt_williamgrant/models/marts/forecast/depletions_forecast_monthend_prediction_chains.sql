{{
  config(
    materialized = 'table'
  )
}}

with projected as (
  select
    fog.processing_year as year,
    fog.processing_month as month,
    to_char(fog.month_date, 'MMMM') as month_name,
    fog.month_date,
    fog.market_name,
    fog.market_code,
    left(fog.distributor_id, 5) as customer_id,
    fog.distributor_id,
    fog.distributor_name,
    fog.parent_chain_name,
    fog.parent_chain_code,
    fog.variant_size_pack_id,
    case when fog.projection_method_used = 'actuals' then false
         when fog.projection_method_used in ('factor_based','straight_line_avg_biz_day') then true
         else false end as is_projected,
    fog.projection_method_used as projection_method_used,
    fog.projected_phys_quantity::float as projected_phys_quantity,
    fog.projected_case_equivalent_quantity::float as projected_case_equivalent_quantity,
    fog.current_month_actual_phys_quantity_todate::float as current_month_actual_phys_quantity_todate,
    fog.current_month_actual_ceq_todate::float as current_month_actual_ceq_todate,
    fog.prev_year_partial_phys_quantity_for_projection::float as prev_year_partial_phys_quantity_for_projection,
    fog.prev_year_partial_ceq_for_projection::float as prev_year_partial_ceq_for_projection,
    fog.prev_year_full_month_phys_quantity_for_projection::float as prev_year_full_month_phys_quantity_for_projection,
    fog.prev_year_full_month_ceq_for_projection::float as prev_year_full_month_ceq_for_projection,
    fog.factor_phys_quantity::float as factor_phys_quantity,
    fog.factor_ceq::float as factor_ceq
  from {{ forecast__monthend_projection('chain', 'window', 3) }} fog
)

select * from projected
order by month_date, market_code, distributor_id, variant_size_pack_id