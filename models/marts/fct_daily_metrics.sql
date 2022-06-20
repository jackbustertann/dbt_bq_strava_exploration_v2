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

{% set hr_zones = (1, 2, 3, 4, 5) %}
{% set pace_zones = (1, 2, 3, 4, 5, 6) %}
{% set power_zones = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11) %}

, activity_zones_wide AS (

    SELECT
        activity_id,

        {% for hr_zone in hr_zones %}
        COALESCE(
            SUM(
                IF(zn.type_index = 0 AND zn.zone_index = {{ hr_zone }}, zn.time, NULL)
            ),
            0
        ) AS time_in_hr_zone_{{ hr_zone }} ,
        {% endfor %}

        {% for pace_zone in pace_zones %}
        COALESCE(
            SUM(
                IF(zn.type_index = 1 AND zn.zone_index = {{ pace_zone }}, zn.time, NULL)
            ),
            0
        ) AS time_in_pace_zone_{{ pace_zone }} ,
        {% endfor %}

        {% for power_zone in power_zones %}
        COALESCE(
            SUM(
                IF(zn.type_index = 2 AND zn.zone_index = {{ power_zone }}, zn.time, NULL)
            ),
            0
        ) AS time_in_power_zone_{{ power_zone }} {% if not loop.last %},{% endif %}
        {% endfor %}

    FROM activity_zones AS zn
    GROUP BY 1
)

---

, final AS (

    SELECT 

        dt.date_day,
        dt.date_week,
        dt.date_month,
        dt.date_quarter,
        dt.date_year,

        act.type,
        act.distance_type,
        act.workout_type,
        act.race_flag,
        act.race_type,

        COALESCE(COUNT(DISTINCT act.id), 0) AS activity_count,
        COALESCE(COUNT(DISTINCT CAST(act.start_date AS date)), 0) AS activity_days_count,


        COALESCE(SUM(act.distance), 0) AS total_distance,
        COALESCE(SUM(act.moving_time), 0) AS total_moving_time,
        COALESCE(SUM(act.elapsed_time), 0) AS total_elapsed_time,
        COALESCE(SUM(act.total_elevation_gain), 0) AS total_elevation_gain,
        COALESCE(SUM(act.kilojoules), 0) AS total_kilojoules,
        COALESCE(SUM(act.suffer_score), 0) AS total_suffer_score,

        COALESCE(
            SUM(act.average_heartrate * act.elapsed_time) / SUM(act.elapsed_time),
            0
        ) AS weighted_avg_heartrate,
        COALESCE(
            SUM(act.average_speed * act.elapsed_time) / SUM(act.elapsed_time),
            0
        ) AS weighted_avg_speed,
        COALESCE(
            SUM(act.average_watts * act.elapsed_time) / SUM(act.elapsed_time),
            0
        ) AS weighted_avg_watts,

        {% for hr_zone in hr_zones %}
        COALESCE(SUM(zn.time_in_hr_zone_{{ hr_zone }}), 0) AS time_in_hr_zone_{{ hr_zone }} ,
        {% endfor %}

        {% for pace_zone in pace_zones %}
        COALESCE(SUM(zn.time_in_pace_zone_{{ pace_zone }}), 0) AS time_in_pace_zone_{{ pace_zone }} ,
        {% endfor %}

        {% for power_zone in power_zones %}
        COALESCE(SUM(zn.time_in_power_zone_{{ power_zone }}), 0) AS time_in_power_zone_{{ power_zone }} {% if not loop.last %},{% endif %}
        {% endfor %}

    FROM activities AS act
    INNER JOIN dates AS dt
        ON CAST(act.start_date AS DATE) = PARSE_DATE('%Y-%m-%d', dt.date_day)
    LEFT JOIN activity_zones_wide AS zn
        ON act.id = zn.activity_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

)

/* final table */
SELECT * FROM final