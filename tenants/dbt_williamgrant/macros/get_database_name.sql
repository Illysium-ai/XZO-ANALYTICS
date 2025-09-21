{% macro get_database_name() %}
    {% if target.name == 'dev' %}
        {{ return('apollo_development') }}
    {% elif target.name == 'prod' %}
        {{ return('apollo_williamgrant') }}
    {% else %}
        {{ return('apollo_development') }}  {# Default to dev database #}
    {% endif %}
{% endmacro %} 