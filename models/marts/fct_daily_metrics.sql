/* import CTE's */
WITH activities AS (

    SELECT * FROM {{ ref('stg_activities') }}

)

, dates AS (

    SELECT * FROM {{ ref('all_dates') }} 

)

, final AS (

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

        COALESCE(COUNT(DISTINCT act.id), 0) AS activity_count,
        COALESCE(COUNT(DISTINCT CAST(act.start_date AS date)), 0) AS activity_days_count,


        COALESCE(SUM(act.distance), 0) AS total_distance,
        ROUND(COALESCE(SUM(act.moving_time), 0) / 60, 2) AS total_moving_time,
        ROUND(COALESCE(SUM(act.elapsed_time), 0) / 60, 2) AS total_elapsed_time,
        COALESCE(SUM(act.total_elevation_gain), 0) AS total_elevation_gain,
        COALESCE(SUM(act.kilojoules), 0) AS total_kilojoules,
        COALESCE(SUM(act.suffer_score), 0) AS total_suffer_score,

        CAST(COALESCE(
            SUM(act.average_heartrate * act.elapsed_time) / SUM(act.elapsed_time),
            0
        ) AS INTEGER) AS weighted_avg_heartrate,
        ROUND(COALESCE(
            SUM(act.average_speed * act.elapsed_time) / SUM(act.elapsed_time),
            0
        ), 2) AS weighted_avg_speed,
        CAST(COALESCE(
            SUM(act.average_watts * act.elapsed_time) / SUM(act.elapsed_time),
            0
        ) AS INTEGER) AS weighted_avg_watts

    FROM activities AS act
    INNER JOIN dates AS dt
        ON CAST(act.start_date AS DATE) = PARSE_DATE('%Y-%m-%d', dt.date_day)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

)

/* final table */
SELECT * FROM final