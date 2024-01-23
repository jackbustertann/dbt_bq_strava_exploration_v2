WITH activities AS (
    SELECT 
        id AS activity_id,
        moving_time_s,
        cast(start_datetime as date) AS start_date
    FROM {{ ref('stg_strava_activities' )}}
    WHERE has_power
        AND id IN (
            SELECT DISTINCT activity_id
            FROM {{ ref('int_activity_streams_with_full_coverage')}}
        )
    ORDER BY start_datetime DESC
),

streams AS (
    SELECT * 
    FROM {{ ref('int_activity_streams_with_full_coverage')}}
),

zones AS (
    SELECT *
    FROM {{ ref('int_power_zones' )}}
),

dates AS (
    SELECT 
        cast(date_day as date) AS date_day,
        cast(date_week as date) AS date_week
    FROM {{ ref("dim_date_details") }}
),

activity_zones AS (
    SELECT 
        activities.activity_id,
        activities.moving_time_s,
        zones.zone_index,
        zones.lower_bound,
        zones.upper_bound
    FROM activities
    JOIN dates
        ON activities.start_date = dates.date_day
    JOIN zones
        ON dates.date_week BETWEEN CAST(zones.start_week AS DATE) AND CAST(zones.end_week AS DATE)
),

moving_time_in_zones AS (
    SELECT 
        activity_zones.activity_id,
        activity_zones.zone_index,
        COUNT(streams.elapsed_time_s) AS moving_time_in_zone_s
    FROM streams
    JOIN activity_zones
        ON streams.activity_id = activity_zones.activity_id
            AND ((streams.power_watts > activity_zones.lower_bound OR activity_zones.lower_bound = 0) 
            AND (streams.power_watts <= activity_zones.upper_bound OR activity_zones.upper_bound = -1))
    group by 1, 2
),

moving_time_in_zones_with_zeros AS (
    SELECT 
        activity_zones.*,
        COALESCE(moving_time_in_zone_s, 0) AS moving_time_in_zone_s
    FROM activity_zones
    LEFT JOIN moving_time_in_zones
        USING(activity_id, zone_index)
),

moving_time_in_zones_with_percents AS (
    SELECT 
    * EXCEPT(moving_time_in_zone_prop),
    ROUND(moving_time_in_zone_prop * 100, 1) AS moving_time_in_zone_percent,
    ROUND((total_moving_time_s/moving_time_s) * 100, 1) AS moving_time_coverage_percent,
    FORMAT('%02d', CAST(FLOOR(moving_time_in_zone_s / 3600) AS INT64)) || ':' || FORMAT('%02d', CAST(FLOOR((moving_time_in_zone_s - (FLOOR(moving_time_in_zone_s / 3600) * 3600)) / 60) AS INT64)) || ':' || FORMAT('%02d', CAST(moving_time_in_zone_s - (FLOOR(moving_time_in_zone_s / 3600) * 3600) - (FLOOR((moving_time_in_zone_s - (FLOOR(moving_time_in_zone_s / 3600) * 3600)) / 60) * 60)  AS INT64)) AS moving_time_in_zone_hhmmss,
    FORMAT('%02d', CAST(FLOOR(total_moving_time_s / 3600) AS INT64)) || ':' || FORMAT('%02d', CAST(FLOOR((total_moving_time_s - (FLOOR(total_moving_time_s / 3600) * 3600)) / 60) AS INT64)) || ':' || FORMAT('%02d', CAST(total_moving_time_s - (FLOOR(total_moving_time_s / 3600) * 3600) - (FLOOR((total_moving_time_s - (FLOOR(total_moving_time_s / 3600) * 3600)) / 60) * 60)  AS INT64)) AS total_moving_time_hhmmss
    FROM (
        SELECT 
            *,
            moving_time_in_zone_s / SUM(moving_time_in_zone_s) OVER(PARTITION BY activity_id) AS moving_time_in_zone_prop,
            SUM(moving_time_in_zone_s) OVER(PARTITION BY activity_id) AS total_moving_time_s
        FROM moving_time_in_zones_with_zeros
    )
)

SELECT 
    * EXCEPT(zone_index),
    CAST(zone_index AS STRING) AS zone_index
FROM moving_time_in_zones_with_percents
ORDER BY 1 DESC, 2
