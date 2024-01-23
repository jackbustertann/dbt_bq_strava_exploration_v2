with
    activity_streams_source as (
        select * from {{ source("strava_prod", "strava_activity_streams") }}
    )

select
    cast(activity_id as string) as activity_id,

    parse_date(
        '%Y-%m-%d', substr(ifnull(last_updated, '2022-05-15'), 1, 10)
    ) as last_updated_date,

    cast(latlng as string) as latlng,

    cast(time as int64) as elapsed_time_s,
    cast(heartrate as int64) as heartrate_bpm,
    cast(cadence as int64) as cadence_rpm,
    cast(temp as int64) as temperature_c,

    cast(distance as float64) as distance_m,
    cast(if(cadence=0, 0, watts) as float64) as power_watts,
    cast(velocity_smooth as float64) as speed_kmhr,
    cast(grade_smooth as float64) as grade_percent,
    cast(altitude as float64) as elevation_m
from activity_streams_source
WHERE ifnull(cast(moving as bool), False)
