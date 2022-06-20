/* setting lineage */
WITH activity_zones AS (

  SELECT * FROM {{ source('strava_dev', 'activity_zones') }}

)

/* casting data types and defining new columns */
, activity_zones_int AS (

    SELECT

    * EXCEPT (last_updated), 

    RANK() OVER(PARTITION BY activity_id, type ORDER BY min, max) AS zone_index,
    CASE 
      WHEN type = 'heartrate' THEN 0
      WHEN type = 'pace' THEN 1
      WHEN type = 'power' THEN 2
      ELSE -1
    END AS type_index,

    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', last_updated) AS last_updated

  FROM activity_zones

)

/* defining primary key */
, activity_zones_final AS (

SELECT 

  *,

  activity_id || CAST(type_index AS STRING) || FORMAT('%02d', zone_index) AS id

FROM activity_zones_int

)

/* creating final table */
SELECT * FROM activity_zones_final