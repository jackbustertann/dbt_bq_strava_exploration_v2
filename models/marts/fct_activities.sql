with
    activities as (select * from {{ ref("stg_strava_activities") }}),

    time_in_heartrate_zones as (
        select * from {{ ref("int_time_in_heartrate_zones_long_to_wide") }}
    ),

    time_in_power_zones as (
        select * from {{ ref("int_time_in_power_zones_long_to_wide") }}
    ),

    best_power_efforts as (
        select * from {{ ref("int_best_power_efforts_long_to_wide") }}
    ),

    dates as (select * from {{ ref("dim_date_details") }}),

    ftp_13w as (select * from {{ ref("int_ftp_13w") }})

select
    -- ids
    act.id,

    -- dimensions (non-categorical)
    act.name,

    -- dimensions (categorical)
    act.sport,

    -- dimensions (boolean)
    act.is_indoor,
    act.has_heartrate,
    act.has_power,
    act.is_manual,
    act.is_race,

    -- dates
    cast(act.start_datetime as date) as start_date,
    act.last_updated_date,

    -- measures (contextual)
    round(act.average_cadence_rpm, 0) as average_cadence_rpm,
    round(act.min_elevation_m, 0) as min_elevation_m,
    round(act.max_elevation_m, 0) as max_elevation_m,
    round(act.elevation_gain_m, 0) as elevation_gain_m,
    act.average_temperature_c,

    -- measures (volume)
    round(act.distance_m / 1000, 2) as distance_km,
    round(act.moving_time_s / 60, 2) as moving_time_mins,
    round(act.elapsed_time_s / 60, 2) as elapsed_time_mins,

    -- measures (intensity)
    round(act.average_heartrate_bpm, 0) as average_heartrate_bpm,
    round(act.max_heartrate_bpm, 0) as max_heartrate_bpm,
    round(act.calories_kj / (act.elapsed_time_s / 3600), 0) as calories_kjhr,
    {% for zone_index in [1, 2, 3, 4, 5] -%}
        hr_zones.moving_time_in_heartrate_zone_percent_{{ zone_index }},
    {% endfor %}

    -- measures (load)
    act.calories_kj,
    act.suffer_score,

    -- measures (performance)
    round(act.average_speed_kmhr, 2) as average_speed_kmhr,
    round(act.max_speed_kmhr, 2) as max_speed_kmhr,
    round(act.average_power_watts, 0) as average_power_watts,
    round(act.normalised_power_watts, 0) as normalised_power_watts,
    round(act.max_power_watts, 0) as max_power_watts,
    round(act.normalised_power_watts / ftp.ftp_watts, 2) as normalised_power_scaled,
    {% for effort_duration in ["15s", "60s", "300s", "600s", "1200s"] -%}
        round(
            if(
                best_pow.best_power_watts_{{ effort_duration }} = 0,
                null,
                best_pow.best_power_watts_{{ effort_duration }}
            ),
            0
        ) as best_power_watts_{{ effort_duration }},
    {% endfor %}
    {% for zone_index in [1, 2, 3, 4, 5, 6] -%}
        power_zones.moving_time_in_power_zone_percent_{{ zone_index }}
        {%- if not loop.last %}, {% endif %}
    {% endfor %}
from activities act
join dates on cast(dates.date_day as date) = cast(act.start_datetime as date)
left join time_in_heartrate_zones hr_zones on act.id = hr_zones.activity_id
left join time_in_power_zones power_zones on act.id = power_zones.activity_id
left join best_power_efforts best_pow on act.id = best_pow.activity_id
left join
    ftp_13w ftp
    on dates.date_week between ftp.start_week and ftp.end_week
    and act.sport = 'Ride'
order by act.start_datetime desc
