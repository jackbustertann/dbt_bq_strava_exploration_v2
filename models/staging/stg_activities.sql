/* setting lineage */
WITH activities AS (

  SELECT * FROM {{ source('strava_prod', 'activities') }}
  
)

/* casting data types */
{% set date_cols = ['start_date', 'start_date_local', 'last_updated'] -%}

, activities_casted AS (

  SELECT

    * EXCEPT (start_date, start_date_local, last_updated, distance, type), 

    ROUND(distance / 1000, 2) AS distance,

    CASE 
      WHEN type = 'VirtualRun' THEN 'Run'
      WHEN type = 'Walk' THEN 'Hike'
      WHEN type = 'VirtualRide' THEN 'Ride'
      ELSE type
    END AS sport,

    TIME(TIMESTAMP_SECONDS(elapsed_time)) AS elapsed_time_hhmmss,
    TIME(TIMESTAMP_SECONDS(moving_time)) AS moving_time_hhmmss,

    {% for date_col in date_cols -%}
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', {{date_col}}) AS {{date_col}},
    {% endfor -%}

    SAFE_CAST(
      IF(
        LENGTH(TRIM(start_latlng, '[]')) > 0, 
        SPLIT(TRIM(start_latlng, '[]'), ',')[OFFSET(0)], 
        NULL
      ) 
      AS FLOAT64
    ) AS start_lat,
    SAFE_CAST(
      IF(
        LENGTH(TRIM(start_latlng, '[]')) > 0, 
        SPLIT(TRIM(start_latlng, '[]'), ',')[OFFSET(1)], 
        NULL
      ) 
      AS FLOAT64
    ) AS start_lng,
    SAFE_CAST(
      IF(
        LENGTH(TRIM(end_latlng, '[]')) > 0, 
        SPLIT(TRIM(end_latlng, '[]'), ',')[OFFSET(0)], 
        NULL
      ) 
      AS FLOAT64
    ) AS end_lat,
    SAFE_CAST(
      IF(
        LENGTH(TRIM(end_latlng, '[]')) > 0, 
        SPLIT(TRIM(end_latlng, '[]'), ',')[OFFSET(1)], 
        NULL
      ) 
      AS FLOAT64
    ) AS end_lng

  FROM activities

)

{% set race_distances = [
    ('1 mile', 1.6),
    ('3 km', 3),
    ('5 km', 5),
    ('5 miles', 8),
    ('10 km', 10),
    ('10 miles', 16),
    ('Half Marathon', 21.1),
    ('Marathon', 42.2) 
    ]
    %}

/* creating custom run dimensions */
, run_dimensions AS (

  SELECT 

      id,

      CASE 
          WHEN distance < 8000 THEN '1: Short'
          WHEN distance < 16000 THEN '2: Mid'
          WHEN distance >= 16000 THEN '3: Long'
      END AS distance_type,
      CASE 
          WHEN REGEXP_CONTAINS(LOWER(name), r'intervals|track|yasoo') THEN '4: Intervals'
          WHEN name IN ('WU', 'WD') THEN '0: WU/WD'
          WHEN average_heartrate < 151 THEN '1: Easy'
          WHEN average_heartrate < 167 THEN '2: Steady'
          WHEN average_heartrate < 183 THEN '3: Tempo'
          WHEN average_heartrate >= 183 THEN '5: Anaerobic'
      END AS workout_type, /*/ add seed for hr zones /*/
      REGEXP_CONTAINS(LOWER(name), r'treadmill') AS is_treadmill,
      REGEXP_CONTAINS(LOWER(name), r'[0-9]{0,2}:?[0-9]{1,2}:[0-9]{2}') AS is_race,
      CASE 
          WHEN REGEXP_CONTAINS(LOWER(name), r'pr') THEN 'Parkrun'
          WHEN REGEXP_CONTAINS(LOWER(name), r'xcl') THEN 'XCL'
          WHEN REGEXP_CONTAINS(LOWER(name), r'mwl') THEN 'MWL'
          WHEN REGEXP_CONTAINS(LOWER(name), r'virtual|tt') THEN 'Time Trial'
      END AS race_type,
      CASE
      {% for i in race_distances %}
          WHEN ABS((distance / 1000.0) - {{i[1]}}) / {{i[1]}} < 0.05 THEN '{{i[0]}}'
      {% endfor %}
      END AS race_distance, 
      REGEXP_EXTRACT(LOWER(name), r'#[0-9]') AS race_number,
      REGEXP_EXTRACT(LOWER(name), r'[0-9]+[a-z]{2}') AS race_position,
      REGEXP_EXTRACT(LOWER(name), r'[0-9]{0,2}:?[0-9]{1,2}:[0-9]{2}') AS race_finish_time
      /*/ add a seed for locations /*/ 

  FROM activities
  WHERE type = 'Run'
)

/* joining intermediate tables */
, activities_final AS (

  SELECT 
    ac.* EXCEPT (workout_type),
    rd.* EXCEPT (id)
  FROM activities_casted ac
  LEFT JOIN run_dimensions rd
    ON ac.id = rd.id

)

/* final table */
SELECT * FROM activities_final