{% macro days_of_supply(end_on_hand, total_sales) %}
case 
    when coalesce({{ total_sales }}, 0) = 0 then null
    else (coalesce({{ end_on_hand }}, 0) / (coalesce({{ total_sales }}, 0) / 30.0))::NUMBER(10,1)
end
{% endmacro %} 