{% macro forecast__calc_trend_factors(rolling_sums_cte_name, latest_complete_month_col='latest_complete_month_date') %}
-- Returns a SELECT that yields (market/distributor/vsp ids, {{ latest_complete_month_col }}, run_rate_3m, trend_factor_3m, trend_factor_6m, trend_factor_12m)
-- rolling_sums_cte_name must expose columns: market_code, distributor_id, variant_size_pack_id, cy_3m_sum, cy_6m_sum, cy_12m_sum, py_3m_sum, py_6m_sum, py_12m_sum, {{ latest_complete_month_col }}
(
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
    {{ latest_complete_month_col }},
    cy_3m_sum / 3 as run_rate_3m,
    least(coalesce(cy_3m_sum / nullif(py_3m_sum, 0), 1.0), 1.5) as trend_factor_3m,
    least(coalesce(cy_6m_sum / nullif(py_6m_sum, 0), 1.0), 1.5) as trend_factor_6m,
    least(coalesce(cy_12m_sum / nullif(py_12m_sum, 0), 1.0), 1.5) as trend_factor_12m
  from {{ rolling_sums_cte_name }}
  where month_date = {{ latest_complete_month_col }}
)
{% endmacro %}
