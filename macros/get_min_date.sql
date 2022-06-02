{% macro get_min_date(model, date_col) %}

    {% set min_date_query %}
        SELECT FORMAT_DATE('%Y-%m-%d', MIN({{ date_col }}))
        FROM {{ target.project }}.{{ target.dataset }}.{{ model }}
    {% endset %}

    {% set results = run_query(min_date_query) %}

    {% if execute %}
    {% set min_date = results.columns[0].values()[0] %}
    {% else %}
    {% set min_date = modules.datetime.datetime.now().strftime('%Y-%m-%d') %}
    {% endif %}

    {{ return(min_date)}}

{% endmacro %}