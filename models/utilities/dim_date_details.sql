-- get min date from activities staging
{% set min_start_date_query %}
    SELECT CAST(MIN(start_datetime) AS DATE) AS min_start_date
    FROM {{ ref("stg_strava_activities") }}
{% endset %}

{% set results = run_query(min_start_date_query) %}

{% if execute %} {% set min_start_date = results.columns[0].values()[0] %}
{% else %} {% set min_start_date = modules.datetime.datetime.now().date().strftime('%Y-%m-%d') %}
{% endif %}

select
    format_date('%Y-%m-%d', date) as date_day,
    format_date('%Y-%m-%d', date_trunc(date, week(monday))) as date_week,
    format_date('%Y-%m', date) as date_month,
    format_date('%Y-%Q', date) as date_quarter,
    format_date('%Y', date) as date_year,
    format_date('%A', date) as date_dayname,
    CASE
        WHEN format_date('%A', date) IN ('Saturday', 'Sunday') THEN True
        ELSE False
    END AS date_is_weekend
from UNNEST(
    GENERATE_DATE_ARRAY(
        CAST('{{ min_start_date }}' AS DATE), 
        CURRENT_DATE('UTC')
    )
) date
