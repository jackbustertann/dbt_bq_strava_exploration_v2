SELECT
  * EXCEPT (start_date, start_date_local), 
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', start_date) AS start_date, 
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', start_date_local) AS start_date_local
FROM
  {{ source('strava_dev', 'activity_laps') }}