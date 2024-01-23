-- with
--     ride_activities as (select * from {# {{ ref("fct_activities") }} #} where sport = 'Ride'),

--     dates as (select * from {# {{ ref("dim_date_details") }} #})

-- select 
--     dt.date_day,
--     dt.date_week,
--     dt.date_month,

--     -- volume metrics
--     -- TO DO: re-factor into activities
--     COALESCE(SUM(act.moving_time) / 60, 0) AS total_moving_time_mins,
--     COALESCE(SUM(CASE WHEN act.is_indoor THEN 0 ELSE act.moving_time END) / 60, 0) AS outdoor_moving_time_mins,
--     COALESCE(SUM(act.distance) / 1000, 0) AS total_distance_km,
--     COALESCE(SUM(CASE WHEN act.is_indoor THEN 0 ELSE act.distance END) / 1000, 0) AS outdoor_distance_km,
--     COALESCE(COUNT(DISTINCT act.id), 0) AS activity_count,
--     COALESCE(COUNT(DISTINCT(CASE WHEN not act.is_indoor THEN act.id ELSE NULL END)), 0) AS outdoor_activity_count,
--     COALESCE(COUNT(DISTINCT(CASE 
--         WHEN act.moving_time/60 >= 150 THEN act.id
--         WHEN NOT act.is_indoor AND act.distance/1000 >= 64 THEN act.id
--         ELSE NULL END
--     )), 0) AS long_activity_count

--     -- intensity metrics

--     -- performance metrics

-- from ride_activities act
-- right join dates dt 
--     on format_date('%Y%m%d', act.start_datetime) = dt.date_key
-- group by 1, 2, 3