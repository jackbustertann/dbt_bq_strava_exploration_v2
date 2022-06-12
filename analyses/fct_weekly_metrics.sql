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

---

, sports AS (

    SELECT DISTINCT act.type AS sport FROM activities act
)

, dates_unioned AS (

    SELECT 

        dt.*,

        sp.sport

    FROM dates AS dt
    CROSS JOIN sports AS sp
)

{% set hr_zones = (1, 2, 3, 4, 5) %}

, activity_zones_wide AS (

    SELECT
        activity_id,

        {% for hr_zone in hr_zones %}
        COALESCE(
            SUM(
                IF(zn.type_index = 0 AND zn.zone_index = {{ hr_zone }}, zn.time, NULL)
            ),
            0
        ) AS time_in_hr_zone_{{ hr_zone }} {% if not loop.last %},{% endif %}
        {% endfor %}

    FROM activity_zones AS zn
    GROUP BY 1
)

---

/* dimensions */

---

, final AS (

    SELECT 

        dt.date_day,
        dt.date_week,
        dt.date_month,
        dt.date_quarter,
        dt.date_year,

        dt.sport,

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
        ) AS weighted_avg_hr,

        {% for hr_zone in hr_zones %}
        COALESCE(SUM(zn.time_in_hr_zone_{{ hr_zone }}), 0) AS time_in_hr_zone_{{ hr_zone }} {% if not loop.last %},{% endif %}
        {% endfor %}

    FROM dates_unioned AS dt
    LEFT JOIN activities AS act
        ON CAST(dt.date_day AS DATE) = EXTRACT(DATE FROM act.start_date)
        AND dt.sport = act.type
    LEFT JOIN activity_zones_wide AS zn
        ON act.id = zn.activity_id
    GROUP BY 1, 2, 3, 4, 5, 6

)

/* final table */
SELECT * FROM final