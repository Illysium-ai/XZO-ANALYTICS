{% macro case_equivalent_type(category) %}
case
    when {{ category }} = 'WINE' then '9L Case'
    when {{ category }} = 'SPIRITS' then '2.25 Gallon Case'
    when {{ category }} = 'BEER' then '24x12oz Case'
    else 'Standard Case'
end
{% endmacro %} 