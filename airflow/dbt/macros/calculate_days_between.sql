{% macro calculate_days_between(start_date, end_date) %}
  case
    when {{ start_date }} is not null and {{ end_date }} is not null
    then {{ end_date }}::date - {{ start_date }}::date
    else null
  end
{% endmacro %}
