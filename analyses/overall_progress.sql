{% set sports = ['ride', 'run'] -%}
{% set metric = 'elapsed_time' -%}
{% set window_size = 9 -%}

WITH weekly_totals_coalesced AS (
SELECT
    dt.date_week,
    {% for sport in sports -%}
    SUM(COALESCE(q_1.{{metric}}_{{sport}}, 0)) AS {{metric}}_{{sport}},
    {% endfor -%}
    SUM(COALESCE(q_1.{{metric}}_total, 0)) AS {{metric}}_total
FROM (
    SELECT 
        date_day,
        {% for sport in sports -%}
        SUM(COALESCE(
            CASE 
                WHEN LOWER(sport) = '{{sport}}' THEN total_{{metric}}
                ELSE 0
            END      
        )) AS {{metric}}_{{sport}},
        {% endfor -%}
        SUM(COALESCE(total_{{metric}}, 0)) AS {{metric}}_total
    FROM {{ ref('fct_daily_metrics') }}
    GROUP BY 1
) q_1
RIGHT JOIN {{ ref('all_dates') }} dt
ON dt.date_day = q_1.date_day
GROUP BY 1
ORDER BY 1 DESC
) 


, weekly_totals_agg_overall AS (
SELECT 
*
FROM (
    SELECT 
        *,
        {% for sport in sports -%}
        {{metric}}_{{sport}}/ ({{metric}}_total + 0.01) AS {{sport}}_percent,
        {% endfor -%}
        {% for sport in sports -%}
        RANK() OVER(ORDER BY {{metric}}_{{sport}} DESC) AS {{sport}}_rank_overall,
        {% endfor -%}
        RANK() OVER(ORDER BY {{metric}}_total DESC) AS total_rank_overall
    FROM weekly_totals_coalesced
    ORDER BY 1 DESC
) AS q_1
 WHERE CAST(date_week AS DATE) = CAST('2023-01-02' AS DATE)
) 

, weekly_totals_agg_window AS (
SELECT
date_week,
total_rank_window,
total_{{metric}}_window
FROM (
    SELECT 
    wt_1.date_week,
    wt_2.date_week AS date_week_other,
    wt_2.{{metric}}_total AS {{metric}}_total_other,
    ROW_NUMBER() OVER(PARTITION BY wt_1.date_week ORDER BY wt_2.{{metric}}_total DESC) AS total_rank_window,
    AVG(wt_2.{{metric}}_total) OVER(PARTITION BY wt_1.date_week) AS total_{{metric}}_window
    FROM weekly_totals_coalesced AS wt_1
    JOIN weekly_totals_coalesced AS wt_2
    ON CAST(wt_2.date_week AS DATE)
        BETWEEN 
            DATE_SUB(CAST(wt_1.date_week AS DATE), INTERVAL {{window_size}} WEEK) 
            AND CAST(wt_1.date_week AS DATE)
    WHERE CAST(wt_1.date_week AS DATE) = CAST('2023-01-02' AS DATE)
    ORDER BY 1 DESC, 2 DESC
) q_1
WHERE CAST(date_week AS DATE) = CAST(date_week_other AS DATE)
)

SELECT 
wt_agg_o.*,
wt_agg_w.total_rank_window,
wt_agg_w.total_{{metric}}_window
FROM weekly_totals_agg_overall wt_agg_o
JOIN weekly_totals_agg_window wt_agg_w
ON CAST(wt_agg_o.date_week AS DATE) = CAST(wt_agg_w.date_week AS DATE)


--- include missing weeks x
--- add dynamic moving average x
--- add yearly rank
--- add jinja for dynamic metrics x