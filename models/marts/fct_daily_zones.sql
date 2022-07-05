/* import CTE's */
WITH activities AS (

    SELECT * FROM {{ ref('stg_activities') }}

)

, activity_zones AS (

    SELECT * FROM {{ ref('stg_activity_zones') }}

)

, dates AS (

    SELECT * FROM {{ ref('all_dates') }} 

)

    SELECT 

        dt.date_day,
        dt.date_week,
        dt.date_month,
        dt.date_quarter,
        dt.date_year,

        act.sport,
        act.distance_type,
        act.workout_type,
        act.race_flag,
        act.race_type,

        zn.type AS zone_type,
        zn.zone_index,

        ROUND(COALESCE(SUM(zn.time), 0) / 60, 2) AS time_in_zone,

    FROM activities AS act
    INNER JOIN dates AS dt
        ON CAST(act.start_date AS DATE) = PARSE_DATE('%Y-%m-%d', dt.date_day)
    LEFT JOIN activity_zones AS zn
        ON act.id = zn.activity_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12