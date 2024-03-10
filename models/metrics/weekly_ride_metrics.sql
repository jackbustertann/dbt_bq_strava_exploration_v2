with
    ride_activities as (select * from {{ ref("fct_activities") }} where sport = 'Ride'),

    dates as (select * from {{ ref("dim_date_details") }} ),

    daily_ride_metrics AS (
        select 
            dt.date_day,
            dt.date_week,
            dt.date_month,
            dt.date_year,

            -- volume metrics
            COALESCE(SUM(act.moving_time_mins), 0) AS total_moving_time_mins,
            COALESCE(SUM(CASE WHEN act.is_indoor THEN 0 ELSE act.moving_time_mins END), 0) AS outdoor_moving_time_mins,
            COALESCE(SUM(act.distance_km), 0) AS total_distance_km,
            COALESCE(SUM(CASE WHEN act.is_indoor THEN 0 ELSE act.distance_km END), 0) AS outdoor_distance_km,
            COALESCE(SUM(act.elevation_gain_m), 0) AS total_elevation_gain_m,
            COALESCE(SUM(CASE WHEN act.is_indoor THEN 0 ELSE act.elevation_gain_m END), 0) AS outdoor_elevation_gain_m

            -- intensity metrics

            -- performance metrics

        from dates dt 
        left join ride_activities act
            on dt.date_day = format_date('%Y-%m-%d', act.start_date)
        group by 1, 2, 3, 4
    )

    SELECT 
        date_week,
        MIN(date_month) AS date_month,
        MIN(date_year) AS date_year,

        ROUND(SUM(total_moving_time_mins), 1) AS total_moving_time_mins,
        ROUND(SUM(outdoor_moving_time_mins), 1) AS outdoor_moving_time_mins,
        ROUND(SUM(total_distance_km), 1) AS total_distance_km,
        ROUND(SUM(outdoor_distance_km), 1) AS outdoor_distance_km,
        ROUND(SUM(total_elevation_gain_m), 1) AS total_elevation_gain_m,
        ROUND(SUM(outdoor_elevation_gain_m), 1) AS outdoor_elevation_gain_m,
        ROUND(COUNT(DISTINCT
            CASE 
                WHEN total_moving_time_mins >= 15 THEN date_day
                ELSE NULL
            END            
        ), 0) AS active_day_count,
        ROUND(COUNT(DISTINCT
            CASE 
                WHEN outdoor_moving_time_mins >= 15 THEN date_day
                ELSE NULL
            END            
        ), 0) AS outdoor_day_count,
        ROUND(COUNT(DISTINCT
            CASE 
                WHEN (total_moving_time_mins >= 180) OR (total_distance_km >= 80) THEN date_day
                ELSE NULL
            END
        ), 0) AS long_day_count,
        ROUND(MAX(total_moving_time_mins), 1) AS max_day_moving_time_mins,
        ROUND(MAX(outdoor_moving_time_mins), 1) AS max_day_outdoor_moving_time_mins,
        ROUND(MAX(total_distance_km), 1) AS max_day_distance_km,
        ROUND(MAX(outdoor_distance_km), 1) AS max_day_outdoor_distance_km
    FROM daily_ride_metrics
    WHERE date_week >= '2022-03-21'
    group by date_week
    ORDER BY 1 DESC