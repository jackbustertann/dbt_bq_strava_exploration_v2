WITH ftp_13w AS (
    SELECT * 
    FROM {{ ref('int_ftp_13w') }}
),

{% set lower_bounds = [0, 0.6, 0.76, 0.9, 1.05, 1.19] %}
{% set upper_bounds = [0.6, 0.76, 0.9, 1.05, 1.19, -1] %}

power_zones AS (
{% for i in range(lower_bounds|length) %}
    SELECT 
        start_week,
        end_week,
        {{ i+1 }} AS zone_index,
        ROUND({{ lower_bounds[i] }} * ftp_watts, 0) AS lower_bound,
        IF({{ upper_bounds[i] }} > 0, ROUND({{ upper_bounds[i] }} * ftp_watts, 0), -1) AS upper_bound
    FROM ftp_13w
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
)

SELECT 
    CONCAT(REPLACE(start_week, '-', '')) || '-' || FORMAT('%02d', CAST(zone_index AS INT)) AS id,
    *
FROM power_zones
ORDER BY start_week DESC, zone_index