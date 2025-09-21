{% macro standardized_size(unit_volume_desc) %}
-- This macro standardizes bottle sizes based on the unit_volume_desc column
-- Returns a consistent format for bottle sizes
case
    when {{ unit_volume_desc }} like '%750%ML%' or {{ unit_volume_desc }} like '%750%ml%' then '750ML'
    when {{ unit_volume_desc }} like '%1L%' or {{ unit_volume_desc }} like '%1LTR%' then '1L'
    when {{ unit_volume_desc }} like '%1.75%' or {{ unit_volume_desc }} like '%1.75LTR%' then '1.75L'
    when {{ unit_volume_desc }} like '%375%ML%' or {{ unit_volume_desc }} like '%375%ml%' then '375ML'
    when {{ unit_volume_desc }} like '%355%ML%' or {{ unit_volume_desc }} like '%12oz%' then '355ML'
    when {{ unit_volume_desc }} like '%500%ML%' or {{ unit_volume_desc }} like '%500%ml%' then '500ML'
    when {{ unit_volume_desc }} like '%187%ML%' or {{ unit_volume_desc }} like '%187%ml%' then '187ML'
    when {{ unit_volume_desc }} like '%700%ML%' or {{ unit_volume_desc }} like '%700%ml%' then '700ML'
    when {{ unit_volume_desc }} like '%200%ML%' or {{ unit_volume_desc }} like '%200%ml%' then '200ML'
    when {{ unit_volume_desc }} like '%100%ML%' or {{ unit_volume_desc }} like '%100%ml%' then '100ML'
    when {{ unit_volume_desc }} like '%50%ML%' or {{ unit_volume_desc }} like '%50%ml%' then '50ML'
    else 'OTHER'
end
{% endmacro %} 