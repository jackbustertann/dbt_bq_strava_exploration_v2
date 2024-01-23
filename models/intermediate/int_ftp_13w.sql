WITH zwift_race_efforts AS (
    SELECT 
        activities.id, 
        CAST(activities.start_datetime AS DATE) AS start_date,
        best_efforts.best_effort AS best_effort_1200s
    FROM {{ ref("stg_strava_activities") }} activities
    LEFT JOIN {{ ref('int_best_power_efforts') }} best_efforts
        ON activities.id = best_efforts.activity_id AND best_efforts.effort_duration_s = '1200s'
    WHERE activities.sport = 'Ride' 
        AND activities.is_race
        AND activities.is_indoor
),

best_efforts_weekly AS (
    SELECT 
        dates.date_week,
        MAX(zwift_race_efforts.best_effort_1200s) AS best_effort_1200s
    FROM {{ ref('dim_date_details' )}} dates
    LEFT JOIN zwift_race_efforts
        ON CAST(dates.date_day AS DATE) = zwift_race_efforts.start_date
    GROUP BY 1
    ORDER BY 1 DESC
),

best_efforts_13w AS (
    SELECT 
        *,
        MAX(best_effort_1200s) over (order by date_week ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS best_effort_1200s_13w
    FROM best_efforts_weekly
    ORDER BY 1 DESC
),

best_efforts_13w_date_bounds AS (
    SELECT 
        start_week,
        MAX(date_week) AS end_week
    FROM (
        SELECT 
            date_week,
            MAX(
                CASE 
                    WHEN best_effort_1200s_13w = best_effort_1200s THEN date_week
                    ELSE NULL
                END
            ) OVER(ORDER BY date_week) AS start_week
        FROM best_efforts_13w
    )
    GROUP BY start_week
)

SELECT 
    date_bounds.start_week,
    date_bounds.end_week,
    ROUND(best_effort_1200s_13w * 0.95,0) AS ftp_watts
FROM best_efforts_13w_date_bounds date_bounds
JOIN best_efforts_13w best_efforts
    ON date_bounds.start_week = best_efforts.date_week
ORDER BY date_bounds.start_week DESC