WITH age_on_first_day_of_week AS (
    SELECT 
        date_week,
        FLOOR(DATE_DIFF(CAST(date_week AS DATE), CAST('1997-06-19' AS DATE), day) / 365) AS age_years
    FROM {{ ref('dim_date_details' )}} dates
    GROUP BY 1
    ORDER BY 1
),

max_hr_date_bounds AS (
    SELECT 
    *,
    ROUND(211 - (0.64 * age_years), 0) AS estimated_max_hr
    FROM (
        SELECT 
            age_years,
            MIN(date_week) AS start_week,
            MAX(date_week) AS end_week
        FROM age_on_first_day_of_week
        GROUP BY 1
        ORDER BY 1
    )
),

{% set lower_bounds = [0, 0.59, 0.78, 0.87, 0.97] %}
{% set upper_bounds = [0.59, 0.78, 0.87, 0.97, -1] %}

hr_zones AS (
{% for i in range(lower_bounds|length) %}
    SELECT 
        start_week,
        end_week,
        {{ i+1 }} AS zone_index,
        ROUND({{ lower_bounds[i] }} * estimated_max_hr, 0) AS lower_bound,
        IF({{ upper_bounds[i] }} > 0, ROUND({{ upper_bounds[i] }} * estimated_max_hr, 0), -1) AS upper_bound
    FROM max_hr_date_bounds
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
)

SELECT 
    CONCAT(REPLACE(start_week, '-', '')) || '-' || FORMAT('%02d', CAST(zone_index AS INT)) AS id,
    *
FROM hr_zones
ORDER BY start_week DESC, zone_index