SELECT
  * 
FROM
  {{ source('strava_dev', 'activity_zones') }}