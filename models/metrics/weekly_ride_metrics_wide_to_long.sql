-- create weekly metrics table
-- with columns (week, sport, metric-name, measure-name, unit, type, aggregate, 1w/6w/13w/26w/52w value, 52w + overall rank)
-- requires tables (metric LOOKUP table, aggregates, ranks)
-- TODO: profile volume metrics
-- TODO: add 6w, 13w, 26w moving ranks
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

    -- calculate 52w rolling ranks
    weekly_ride_metrics_52w_rolling_ranks_wide AS (
        SELECT 
            * EXCEPT (date_week_other)
        FROM (
            SELECT 
                a.date_week,
                b.date_week AS date_week_other,
                {% for i in range(measure_names|length) %}
                    {% set measure_name = measure_names[i] %}
                RANK() OVER(PARTITION BY a.date_week ORDER BY b.{{ measure_name }} DESC, b.date_week ASC) as {{ measure_name }}_rank_52w{% if not loop.last %}, {% endif %}
                {% endfor %}
            FROM weekly_ride_metrics a
            JOIN weekly_ride_metrics b
                ON CAST(b.date_week AS DATE) BETWEEN DATE_SUB(CAST(a.date_week AS DATE), INTERVAL 51 WEEK) AND CAST(a.date_week AS DATE)
            ORDER BY a.date_week DESC, b.date_week DESC
        ) AS weekly_ride_metrics_52w_exploded
        WHERE date_week = date_week_other
    ),

    weekly_ride_metrics_52w_rolling_ranks_long AS (
        {% for i in range(measure_names|length) %}
            {% set measure_name = measure_names[i] %}
        SELECT 
            date_week, 
            '{{ measure_name }}' AS measure_name,
            {{ measure_name }}_rank_52w AS metric_rank_52w
            {%- if not loop.last %}, {% endif %}
        FROM weekly_ride_metrics_52w_rolling_ranks_wide
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
        ranks.metric_rank_52w
    FROM weekly_ride_metrics_long weekly_metrics
    JOIN metric_definitions def
        ON weekly_metrics.measure_name = def.measure_name
    LEFT JOIN weekly_ride_metrics_rolling_aggs_long aggs
    ON weekly_metrics.date_week = aggs.date_week AND weekly_metrics.measure_name = aggs.measure_name
    LEFT JOIN weekly_ride_metrics_52w_rolling_ranks_long ranks
    ON weekly_metrics.date_week = ranks.date_week AND weekly_metrics.measure_name = ranks.measure_name
    ORDER BY weekly_metrics.date_week DESC





