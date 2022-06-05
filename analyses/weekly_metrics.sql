SELECT 
    dates.date_day,
    dates.date_week,
    dates.date_month,
    dates.date_quarter,
    dates.date_year,
    COUNT(DISTINCT activities.id) AS activity_count,
    SUM(activities.distance) AS total_distance
FROM {{ ref('all_dates') }} AS dates
LEFT JOIN {{ ref('stg_activities') }} AS activities
    ON CAST(dates.date_day AS DATE) = EXTRACT(DATE FROM activities.start_date)
GROUP BY 1, 2, 3, 4, 5