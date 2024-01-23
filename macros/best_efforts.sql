{% macro get_best_efforts(
    effort_durations,
    measure_col,
    activity_ids
) %}
    -- macro for calculating activity best effort based on streaming data

    WITH activity_streams_with_full_coverage AS (
        select * 
        from {{ ref('int_activity_streams_with_full_coverage') }}
        WHERE CAST(activity_id AS INT64) IN ({{ activity_ids | join(',') }})
    ),

    activity_efforts as (
        {% for effort_duration in effort_durations %}
            -- calculate effort duration minus one
            {% set effort_duration_minus_one = effort_duration - 1 %}

            select 
                *,
                (avg_measure_value * effort_coverage) / effort_duration AS weighted_avg_measure_value
            from (
                select
                    activity_id,
                    {{ effort_duration }} as effort_duration,
                    min(elapsed_time_s) over (
                        partition by activity_id
                        order by
                            elapsed_time_s
                            rows
                            between {{ effort_duration_minus_one }} preceding
                            and current row
                    ) as start_time,
                    max(elapsed_time_s) over (
                        partition by activity_id
                        order by
                            elapsed_time_s
                            rows
                            between {{ effort_duration_minus_one }} preceding
                            and current row
                    ) as end_time,
                    sum(if(is_recorded, 1, 0)) over (
                        partition by activity_id
                        order by
                            elapsed_time_s rows
                            between {{ effort_duration_minus_one }} preceding
                            and current row
                    ) as effort_coverage,
                    {{ measure_col }} AS measure_value,
                    avg({{ measure_col }}) over (
                        partition by activity_id
                        order by
                            elapsed_time_s rows
                            between {{ effort_duration_minus_one }} preceding
                            and current row
                    ) as avg_measure_value
                from activity_streams_with_full_coverage
            )
            {% if not loop.last %}
            union all
            {% endif %}
        {% endfor %}
    ),

    activity_efforts_ranked AS (
        SELECT 
            *,
            ROW_NUMBER() OVER(
                PARTITION BY activity_id, effort_duration
                ORDER BY weighted_avg_measure_value DESC, effort_coverage DESC
            ) AS effort_rank
        FROM activity_efforts
    )

    SELECT 
        activity_id,
        CONCAT(CAST(CAST(effort_duration AS INT64) AS STRING), 's') as effort_duration_s,
        '{{ measure_col }}' as measure_type,
        start_time,
        end_time,
        weighted_avg_measure_value AS best_effort,
        effort_coverage
    FROM activity_efforts_ranked
    WHERE effort_rank = 1
    ORDER BY activity_id, effort_duration

{% endmacro %}
