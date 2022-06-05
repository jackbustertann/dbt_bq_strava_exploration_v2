{{
  config(
    materialized = 'incremental',
    unique_key = 'id'
  )
}}

/* setting lineage */
WITH activities AS (
  SELECT * 
  FROM {{ source('strava_dev', 'activities') }}
  {% if is_incremental() %}
  WHERE TIMESTAMP_ADD(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', start_date), INTERVAL 3 DAY) > (SELECT MAX(start_date) FROM {{ this }})
  {% endif %} 
)

/* casting data types and defining new columns */
{% set date_cols = ['start_date', 'start_date_local', 'last_updated'] -%}

, activities_final AS (
  SELECT
    * EXCEPT (start_date, start_date_local, last_updated), 
    {% for date_col in date_cols -%}
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', {{date_col}}) AS {{date_col}},
    {% endfor -%}
    SAFE_CAST(IF(LENGTH(TRIM(start_latlng, '[]')) > 0, SPLIT(TRIM(start_latlng, '[]'), ',')[OFFSET(0)], NULL) AS FLOAT64) AS start_lat,
    SAFE_CAST(IF(LENGTH(TRIM(start_latlng, '[]')) > 0, SPLIT(TRIM(start_latlng, '[]'), ',')[OFFSET(1)], NULL) AS FLOAT64) AS start_lng,
    SAFE_CAST(IF(LENGTH(TRIM(end_latlng, '[]')) > 0, SPLIT(TRIM(end_latlng, '[]'), ',')[OFFSET(0)], NULL) AS FLOAT64) AS end_lat,
    SAFE_CAST(IF(LENGTH(TRIM(end_latlng, '[]')) > 0, SPLIT(TRIM(end_latlng, '[]'), ',')[OFFSET(1)], NULL) AS FLOAT64) AS end_lng
  FROM
    activities
)

SELECT * FROM activities_final