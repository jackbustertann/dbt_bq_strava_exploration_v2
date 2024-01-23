-- create activity stream table with full coverage
-- imputing missing values with previous non-null value (b-fill)
-- add flags for bfilled values

-- observations
-- outdoor rides have less coverage than zwift
-- recording intervals vary up to 10s
-- any recording interval gretear than 10s can assumed to be a break
{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

{%- set sql_statement -%}
    select id, elapsed_time_s
    FROM {{ ref("stg_strava_activities")}}
    WHERE id in (
        SELECT DISTINCT activity_id
        FROM {{ ref("stg_strava_activity_streams" )}}
    )
    {% if is_incremental() %}
    AND last_updated_date >= (
        SELECT MAX(last_updated_date)
        FROM {{ this }}
    )
    -- AND id NOT IN (
    --     SELECT DISTINCT activity_id
    --     FROM {# {{ this }} #}
    -- )
    {% endif %}
    ORDER BY start_datetime
    LIMIT 50
{%- endset -%}

{%- set query_result = run_query(sql_statement) -%}

{%- if execute -%}
    {% set activity_id_lst = (query_result.columns[0].values() | list) %}
    {% set elapsed_time_lst = (query_result.columns[1].values() | list) %}
{%- endif %}

with activity_streams AS (
    SELECT *
    FROM {{ ref("stg_strava_activity_streams")}}
),

activity_streams_with_full_coverage as (
    {% for i in range(activity_id_lst | length) -%}
        {%- set activity_id = activity_id_lst[i] | string %}
        {%- set elapsed_time = elapsed_time_lst[i] | int %}
        SELECT 
            '{{ activity_id }}' AS activity_id, 
            elapsed_time_filled AS elapsed_time_s,
            if(activity_id IS NOT NULL, True, False) AS is_recorded,
            streams.* EXCEPT(activity_id, elapsed_time_s)
        FROM UNNEST(generate_array(0, {{ elapsed_time }})) elapsed_time_filled
        LEFT JOIN activity_streams streams
        ON elapsed_time_filled = elapsed_time_s AND activity_id = '{{ activity_id }}'
        {% if not loop.last %}
        union all
        {% endif %}
    {%- endfor %}
)

SELECT 
    activity_id || '-' || CAST(CAST(elapsed_time_s AS INT64) AS STRING) AS id,
    *
FROM activity_streams_with_full_coverage
