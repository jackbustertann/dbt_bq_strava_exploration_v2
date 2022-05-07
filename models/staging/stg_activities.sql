SELECT
  * EXCEPT (start_date, start_date_local), 
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', start_date) AS start_date, 
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', start_date_local) AS start_date_local,
  CAST(SPLIT(TRIM(start_latlng, '[]'), ',')[OFFSET(0)] AS FLOAT64) AS start_lat,
  CAST(SPLIT(TRIM(start_latlng, '[]'), ',')[OFFSET(1)] AS FLOAT64) AS start_lng,
  CAST(SPLIT(TRIM(end_latlng, '[]'), ',')[OFFSET(0)] AS FLOAT64) AS end_lat,
  CAST(SPLIT(TRIM(end_latlng, '[]'), ',')[OFFSET(1)] AS FLOAT64) AS end_lng
FROM
  {{ source('strava_dev', 'activities') }}