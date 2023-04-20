{% macro data_local() %}
    date(current_timestamp at time zone 'GMT-3')
{% endmacro %}