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

, dates_concat AS (

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

,   run_dimensions AS (

    SELECT 

        id,

        CASE 
            WHEN distance < 8000 THEN 'Short'
            WHEN distance < 16000 THEN 'Mid'
            WHEN distance >= 16000 THEN 'Long'
        END AS distance_type,
        CASE 
            WHEN REGEXP_CONTAINS(LOWER(name), r'intervals|track|yasoo') THEN 'Intervals'
            WHEN name IN ('WU', 'WD') THEN 'WU/WD'
            WHEN average_heartrate < 151 THEN 'Easy'
            WHEN average_heartrate < 167 THEN 'Steady'
            WHEN average_heartrate < 183 THEN 'Tempo'
            WHEN average_heartrate >= 183 THEN 'Anaerobic'
        END AS workout_type, /*/ add seed for hr zones /*/
        REGEXP_CONTAINS(LOWER(name), r'[0-9]{0,2}:?[0-9]{1,2}:[0-9]{2}') AS race_flag,
        CASE 
            WHEN REGEXP_CONTAINS(LOWER(name), r'pr') THEN 'Parkrun'
            WHEN REGEXP_CONTAINS(LOWER(name), r'xcl') THEN 'XCL'
            WHEN REGEXP_CONTAINS(LOWER(name), r'mwl') THEN 'MWL'
            WHEN REGEXP_CONTAINS(LOWER(name), r'virtual|tt') THEN 'Time Trial'
        END AS race_type,
        CASE
            WHEN ABS(distance - 1600) / 1600 < 0.05 THEN '1 mile'
            WHEN ABS(distance - 3000) / 3000 < 0.05 THEN '3 km'
            WHEN ABS(distance - 5000) / 5000 < 0.05 THEN '5 km'
            WHEN ABS(distance - 8000) / 8000 < 0.05 THEN '5 miles'
            WHEN ABS(distance - 10000) / 10000 < 0.05 THEN '10 km'
            WHEN ABS(distance - 16000) / 16000 < 0.05 THEN '10 miles'
            WHEN ABS(distance - 21100) / 21100 < 0.05 THEN 'Half Marathon'
            WHEN ABS(distance - 42200) / 42200 < 0.05 THEN 'Marathon'
        END AS race_distance, /*/ make dynamic /*/
        REGEXP_EXTRACT(LOWER(name), r'#[0-9]') AS race_number,
        REGEXP_EXTRACT(LOWER(name), r'[0-9]+[a-z]{2}') AS race_position,
        REGEXP_EXTRACT(LOWER(name), r'[0-9]{0,2}:?[0-9]{1,2}:[0-9]{2}') AS race_finish_time
        /*/ add a seed for locations /*/ 

    FROM {{ ref('stg_activities') }}
    WHERE type = 'Run'
    ORDER BY start_date DESC
)

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

    FROM dates_concat AS dt
    LEFT JOIN activities AS act
        ON CAST(dt.date_day AS DATE) = EXTRACT(DATE FROM act.start_date)
        AND dt.sport = act.type
    LEFT JOIN activity_zones_wide AS zn
        ON act.id = zn.activity_id
    GROUP BY 1, 2, 3, 4, 5, 6

)

/* final table */
SELECT * FROM final