{% set min_date = get_min_date(model = 'activities', date_col = 'start_date') %}

WITH all_dates AS (
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('" + min_date + "' as date)",
    end_date="DATE_ADD(cast(CURRENT_DATE('UTC') as date), INTERVAL 1 DAY)"
   )
}}
)

SELECT 
    FORMAT_DATE('%Y-%m-%d', date_day) AS date_day
,   FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(date_day, WEEK(MONDAY))) AS date_week
,   FORMAT_DATE('%Y-%m', date_day) AS date_month
,   FORMAT_DATE('%Y-%Q', date_day) AS date_quarter
,   FORMAT_DATE('%Y', date_day) AS date_year
FROM all_dates