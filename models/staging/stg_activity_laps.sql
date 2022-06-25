/* setting lineage */
WITH activity_laps AS (

  SELECT * FROM {{ source('strava_prod', 'activity_laps') }}

)

/* casting data types and defining new columns */
{%- set date_cols = ['start_date', 'start_date_local', 'last_updated'] -%}

, activity_laps_final AS (

  SELECT

    * EXCEPT (start_date, start_date_local, last_updated), 

    {% for date_col in date_cols -%}
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', {{date_col}}) AS {{date_col}}{% if not loop.last %},{% endif %}
    {% endfor -%}

  FROM activity_laps

)

/* creating final table */
SELECT * FROM activity_laps_final