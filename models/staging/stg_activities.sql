{%- set date_cols = ['start_date', 'start_date_local', 'last_updated'] -%}

WITH activities AS (
  SELECT
    * EXCEPT (start_date, start_date_local, last_updated), 
    {% for date_col in date_cols -%}
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', {{date_col}}) AS {{date_col}},
    {% endfor -%}
    CAST(SPLIT(TRIM(start_latlng, '[]'), ',')[OFFSET(0)] AS FLOAT64) AS start_lat,
    CAST(SPLIT(TRIM(start_latlng, '[]'), ',')[OFFSET(1)] AS FLOAT64) AS start_lng,
    CAST(SPLIT(TRIM(end_latlng, '[]'), ',')[OFFSET(0)] AS FLOAT64) AS end_lat,
    CAST(SPLIT(TRIM(end_latlng, '[]'), ',')[OFFSET(1)] AS FLOAT64) AS end_lng
  FROM
    {{ source('strava_dev', 'activities') }}
  {# {{ limit_dev() }} #}
)

SELECT * FROM activities