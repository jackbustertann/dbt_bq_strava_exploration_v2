{#
  config(
    materialized = 'incremental',
    unique_key = 'id'
  )
#}

/* setting lineage */
-- WITH activities AS (
--   SELECT * 
--   FROM {{ source('strava_dev', 'activities') }}
--   {% if is_incremental() %}
--   WHERE TIMESTAMP_ADD(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', start_date), INTERVAL 3 DAY) > (SELECT MAX(start_date) FROM {{ this }})
--   {% endif %} 
-- )