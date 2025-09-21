{% macro case_equivalent_factor(ml_per_case) %}
-- This macro calculates the case equivalent factor to normalize to 9-liter case equivalents for spirits
-- Uses existing ext_mlp_case column from wg_vip_s3.itm_2_da
-- Compute the total ML per Case for other sources
-- Returns the factor to convert to 9L case equivalents

-- Convert ML volume per case to liters then divide by 9 to get case equivalent factor
({{ ml_per_case }}::float / 1000.0 / 9.0)::float
{% endmacro %} 