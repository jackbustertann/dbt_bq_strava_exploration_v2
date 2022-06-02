{%- set date_cols = ['start_date', 'start_date_local', 'last_updated'] -%}

WITH activity_laps AS (
  SELECT
    * EXCEPT (start_date, start_date_local, last_updated), 
    {% for date_col in date_cols -%}
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', {{date_col}}) AS {{date_col}}{% if not loop.last %},{% endif %}
    {% endfor -%}
  FROM
    {{ source('strava_dev', 'activity_laps') }}
  {# {{ limit_dev() }} #}
)

SELECT * FROM activity_laps