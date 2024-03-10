-- set lineage
with
    activities_source as (
        select * from {{ source("strava_prod", "strava_activities") }}
    ),

    -- create basic case when dimensions
    activities_cases as (
        select
            id,
            case
                when regexp_contains(lower(type), 'run')
                then 'Run'
                when regexp_contains(lower(type), 'ride')
                then 'Ride'
                else type
            end as sport,
            case
                when
                    regexp_contains(
                        lower(name), r'treadmill|indoor|zwift|spin|digme|pyscle'
                    )
                then true
                when regexp_contains(lower(type), 'virtual')
                then true
                else false
            end as is_indoor,
            case
                when
                    id in (
                        '1985636421',
                        '2028511938',
                        '2001078643',
                        '2271688574',  -- (un-realistic max HR)
                        '8169509675',  -- (race effort with un-realistic average HR)
                        '3061139289',
                        '2691579673'  -- (avg/max ratio very low -> spiky HR line)
                    )
                then false
                else ifnull(has_heartrate, false)
            end as has_heartrate,
            case
                when substr(start_date_local, 1, 10) <= '2022-10-23'  -- (mis-configured power meter)
                then false
                else ifnull(device_watts, false)
            end as has_power,
            ifnull(manual, false) as is_manual,
            case
                when regexp_contains(lower(name), r'race:') then true else false
            end as is_race
        from activities_source
    ),

    activities_imputes AS (
        SELECT '9423190867' AS id, 55000 AS distance_m, 1200 AS elevation_gain_m
    ),

    -- rename, cast and handle nulls
    activities_staged as (
        select
            -- ids
            cast(act_src.id as string) as id,

            -- dimensions (non-categorical)
            cast(act_src.name as string) as name,

            -- dimensions (categorical)
            cast(act_case.sport as string) as sport,
            -- activity type 
            -- ride
            -- zwift
            -- spin
            -- gym
            -- outdoor ride
            -- run
            -- treadmill
            -- intervals
            -- time trial
            -- race
            -- outdoor run
            -- activity sub-type
            -- zwift
            -- workout
            -- free-ride
            -- group-ride
            -- race
            -- spin
            -- digme
            -- pyscle
            -- intervals
            -- track
            -- road
            -- time trial
            -- track
            -- road
            -- race
            -- track
            -- road
            -- xc
            -- volume zone
            -- intensity zone
            -- performance zone
            -- dimensions (boolean)
            cast(act_case.is_indoor as boolean) as is_indoor,
            cast(act_case.has_heartrate as boolean) as has_heartrate,
            cast(act_case.has_power as boolean) as has_power,
            cast(IF(act_imp.id IS NOT NULL, True, act_case.is_manual) as boolean) as is_manual,
            cast(act_case.is_race as boolean) as is_race,

            -- dates
            parse_datetime(
                '%Y-%m-%dT%H:%M:%SZ', act_src.start_date_local
            ) as start_datetime,
            parse_date(
                '%Y-%m-%d', substr(ifnull(act_src.last_updated, '2022-05-15'), 1, 10)
            ) as last_updated_date,

            -- measures (float)
            cast(
                COALESCE(
                    act_imp.distance_m,
                    if(act_src.distance = 0, null, act_src.distance)
                ) as float64
            ) as distance_m,
            cast(
                if(act_src.moving_time = 0, null, act_src.moving_time) as float64
            ) as moving_time_s,
            cast(
                if(act_src.elapsed_time = 0, null, act_src.elapsed_time) as float64
            ) as elapsed_time_s,
            cast(
                if(act_src.total_elevation_gain = 0, null, act_src.elev_high) as float64
            ) as max_elevation_m,
            cast(
                if(act_src.total_elevation_gain = 0, null, act_src.elev_low) as float64
            ) as min_elevation_m,
            cast(
                COALESCE(
                    act_imp.elevation_gain_m,
                    if(act_src.total_elevation_gain = 0, null, act_src.total_elevation_gain) 
                ) as float64
            ) as elevation_gain_m,
            cast(act_src.average_temp as float64) as average_temperature_c,
            cast(
                if(act_src.average_speed = 0, null, act_src.average_speed) as float64
            ) as average_speed_kmhr,
            cast(
                if(act_src.max_speed = 0, null, act_src.max_speed) as float64
            ) as max_speed_kmhr,
            cast(
                if(
                    act_src.average_cadence = 0, null, act_src.average_cadence
                ) as float64
            ) as average_cadence_rpm,
            cast(
                if(
                    act_src.kilojoules = 0 or not act_case.has_heartrate,
                    null,
                    act_src.kilojoules
                ) as float64
            ) as calories_kj,
            cast(
                if(
                    act_src.average_heartrate = 0 or not act_case.has_heartrate,
                    null,
                    act_src.average_heartrate
                ) as float64
            ) as average_heartrate_bpm,
            cast(
                if(
                    act_src.max_heartrate = 0 or not act_case.has_heartrate,
                    null,
                    act_src.max_heartrate
                ) as float64
            ) as max_heartrate_bpm,
            cast(
                if(
                    act_src.suffer_score = 0 or not act_case.has_heartrate,
                    null,
                    act_src.suffer_score
                ) as float64
            ) as suffer_score,
            cast(
                if(
                    act_src.average_watts = 0 or not act_case.has_power,
                    null,
                    act_src.average_watts
                ) as float64
            ) as average_power_watts,
            cast(
                if(
                    act_src.weighted_average_watts = 0 or not act_case.has_power,
                    null,
                    act_src.weighted_average_watts
                ) as float64
            ) as normalised_power_watts,
            cast(
                if(
                    act_src.max_watts = 0 or not act_case.has_power,
                    null,
                    act_src.max_watts
                ) as float64
            ) as max_power_watts

        from activities_source act_src
        join activities_cases act_case on act_src.id = act_case.id
        LEFT JOIN activities_imputes act_imp on act_imp.id = act_case.id
        where act_case.sport in ('Run', 'Ride')
    )

-- final table
select *
from activities_staged
