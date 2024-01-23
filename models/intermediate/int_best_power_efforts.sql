{%- set sql_statement -%}
    select id
    FROM {{ ref("stg_strava_activities")}}
    WHERE has_power
        AND id in (
            SELECT DISTINCT activity_id
            FROM {{ ref("stg_strava_activity_streams" )}}
        )
{%- endset -%}

{%- set query_result = run_query(sql_statement) -%}

{%- if execute -%}
    {% set activity_id_lst = (query_result.columns[0].values() | list) %}
{%- endif %}

{{
    get_best_efforts(
        effort_durations = [15, 60, 300, 600, 1200],
        measure_col = 'power_watts',
        activity_ids = activity_id_lst
    )
}}
