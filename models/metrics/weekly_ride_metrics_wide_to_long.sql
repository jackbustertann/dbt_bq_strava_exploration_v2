-- create weekly metrics table
-- with columns (week, sport, metric-name, measure-name, unit, type, aggregate, 1w/6w/13w/26w/52w value, 6w/13w/26w/52w + overall rank)
-- requires tables (metric LOOKUP table, aggregates, ranks)
-- TODO: add metric ID + update joins
-- TODO: add intensity + performance metrics
-- TODO: add sport as parameter

{% set periods = [6, 13, 26, 52] %}

{%- set sql_statement -%}
    select measure_name, aggregate
    FROM {{ ref('dim_metrics') }}
{%- endset -%}

{%- set query_result = run_query(sql_statement) -%}

{%- if execute -%}
    {% set measure_names = (query_result.columns[0].values() | list) %}
    {% set aggregates = (query_result.columns[1].values() | list) %}
{%- endif %}

WITH metric_definitions AS (
    SELECT * FROM {{ ref('dim_metrics') }}
),

    weekly_ride_metrics AS (
        SELECT * FROM {{ ref('weekly_ride_metrics' )}} 
    ),

    -- calculate 6w/13w/26w/52w rolling aggregates 
    weekly_ride_metrics_rolling_aggs_wide AS (
        SELECT 
        date_week,
        {% for i in range(measure_names|length) %}
            {% set measure_name = measure_names[i] %}
            {% set aggregate = aggregates[i] %}
            {% for period in periods %}
                {% set period_minus_one = period - 1 %}
        {{ aggregate }}({{ measure_name }}) OVER(ORDER BY date_week ROWS BETWEEN {{ period_minus_one }} PRECEDING AND CURRENT ROW) as {{ measure_name }}_agg_{{ period }}w
        {%- if not loop.last %}, {% endif %}
            {% endfor %}
        {%- if not loop.last %}, {% endif %}
        {% endfor %}
        FROM weekly_ride_metrics
    ),

    weekly_ride_metrics_rolling_aggs_long AS (
    {% for i in range(measure_names|length) %}
        {% set measure_name = measure_names[i] %}
        {% set aggregate = aggregates[i] %}
        SELECT 
            date_week, 
            '{{ measure_name }}' AS measure_name, 
            {% for period in periods %}
            {{ measure_name }}_agg_{{ period }}w AS metric_agg_{{ period }}w
            {%- if not loop.last %}, {% endif %}
            {% endfor %}
        FROM weekly_ride_metrics_rolling_aggs_wide
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
    ),

    weekly_ride_metrics_rolling_ranks_wide AS (
    {% for period in periods %}
        {% set period_minus_one = period - 1 %}
        SELECT 
            * EXCEPT (date_week_other)
        FROM (
            SELECT 
                a.date_week,
                b.date_week AS date_week_other,
                '{{ period }}' AS period,
                {% for i in range(measure_names|length) %}
                    {% set measure_name = measure_names[i] %}
                RANK() OVER(PARTITION BY a.date_week ORDER BY b.{{ measure_name }} DESC, b.date_week ASC) as {{ measure_name }}_rank{% if not loop.last %}, {% endif %}
                {% endfor %}
            FROM weekly_ride_metrics a
            JOIN weekly_ride_metrics b
                ON CAST(b.date_week AS DATE) BETWEEN DATE_SUB(CAST(a.date_week AS DATE), INTERVAL {{ period_minus_one }} WEEK) AND CAST(a.date_week AS DATE)
            ORDER BY a.date_week DESC, b.date_week DESC
        ) AS weekly_ride_metrics_{{ period }}w_exploded
        WHERE date_week = date_week_other
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
    ),

    weekly_ride_metrics_rolling_ranks_long AS (
    {% for i in range(measure_names|length) %}
        {% set measure_name = measure_names[i] %}
        SELECT 
            date_week, 
            '{{ measure_name }}' AS measure_name,
            {{
                dbt_utils.pivot(
                    "period",
                    periods,
                    agg="max",
                    then_value=measure_name + '_rank',
                    prefix="metric_rank_",
                    suffix="w",
                )
            }}
        FROM weekly_ride_metrics_rolling_ranks_wide
        GROUP BY 1, 2
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
    ),

    weekly_ride_metrics_overall_rolling_ranks_wide AS (
        SELECT 
            * EXCEPT (date_week_other)
        FROM (
            SELECT 
                a.date_week,
                b.date_week AS date_week_other,
                {% for i in range(measure_names|length) %}
                    {% set measure_name = measure_names[i] %}
                RANK() OVER(PARTITION BY a.date_week ORDER BY b.{{ measure_name }} DESC, b.date_week ASC) as {{ measure_name }}_overall_rank{% if not loop.last %}, {% endif %}
                {% endfor %}
            FROM weekly_ride_metrics a
            JOIN weekly_ride_metrics b
                ON CAST(b.date_week AS DATE) <= CAST(a.date_week AS DATE)
            ORDER BY a.date_week DESC, b.date_week DESC
        ) AS weekly_ride_metrics_exploded
        WHERE date_week = date_week_other
    ),

    weekly_ride_metrics_overall_rolling_ranks_long AS (
    {% for i in range(measure_names|length) %}
        {% set measure_name = measure_names[i] %}
        SELECT 
            date_week, 
            '{{ measure_name }}' AS measure_name,
            {{ measure_name }}_overall_rank AS metric_rank_overall
        FROM weekly_ride_metrics_overall_rolling_ranks_wide
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
    ),

    -- convert weekly metrics from wide to long format
    weekly_ride_metrics_long AS (
        {% for i in range(measure_names|length) %}
            {% set measure_name = measure_names[i] %}
        SELECT 
            date_week, 
            date_year,
            '{{ measure_name }}' AS measure_name,
            {{ measure_name }} AS metric_value
            {%- if not loop.last %}, {% endif %}
        FROM weekly_ride_metrics
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
    )

    -- combine weekly metrics with rolling aggs + ranks
    SELECT 
        weekly_metrics.date_week,
        def.metric_name,
        weekly_metrics.metric_value,
        {% for period in periods %}
        aggs.metric_agg_{{ period }}w,
        {% endfor %}
        {% for period in periods %}
        ranks.metric_rank_{{ period }}w,
        {% endfor %}
        overall_ranks.metric_rank_overall
    FROM weekly_ride_metrics_long weekly_metrics
    JOIN metric_definitions def
        ON weekly_metrics.measure_name = def.measure_name
    LEFT JOIN weekly_ride_metrics_rolling_aggs_long aggs
        ON weekly_metrics.date_week = aggs.date_week AND weekly_metrics.measure_name = aggs.measure_name
    LEFT JOIN weekly_ride_metrics_rolling_ranks_long ranks
        ON weekly_metrics.date_week = ranks.date_week AND weekly_metrics.measure_name = ranks.measure_name
    LEFT JOIN weekly_ride_metrics_overall_rolling_ranks_long overall_ranks
        ON weekly_metrics.date_week = overall_ranks.date_week AND weekly_metrics.measure_name = overall_ranks.measure_name
    ORDER BY weekly_metrics.date_week DESC





