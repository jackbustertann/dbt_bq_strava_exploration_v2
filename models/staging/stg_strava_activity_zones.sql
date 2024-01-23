-- set lineage
with
    activity_zones_source as (

        select * from {{ source("strava_prod", "strava_activity_zones") }}

    ),

    -- create primary key
    activity_zones_with_primary_key as (
        select
            *,
            concat(
                cast(activity_id as string),
                cast(zone_type_index as string),
                cast(cast(zone_index as int64) as string)
            ) as id
        from
            (
                select
                    *,
                    case
                        when lower(type) = 'heartrate'
                        then 1
                        when lower(type) = 'pace'
                        then 2
                        when lower(type) = 'power'
                        then 3
                    end as zone_type_index,
                    rank() over (
                        partition by activity_id, type order by distribution_buckets_min, distribution_buckets_max
                    ) as zone_index
                from activity_zones_source
            ) as activity_zones_with_indices
    ),

    -- rename, cast and impute nulls
    activity_zones_staged as (
        select
            cast(id as string) as id,
            cast(activity_id as string) as activity_id,
            cast(sensor_based as boolean) as has_sensor,
            parse_date(
                '%Y-%m-%d', substr(ifnull(last_updated, '2022-05-15'), 1, 10)
            ) as last_updated_date,
            cast(initcap(type) as string) as zone_type_name,
            cast(zone_type_index as string) as zone_type_index,
            cast(zone_index as int64) as zone_index,
            cast(distribution_buckets_min as float64) as zone_lower,
            cast(distribution_buckets_max as float64) as zone_upper,
            cast(if(sensor_based, distribution_buckets_time, null) as int64) as time_in_zone_s
        from activity_zones_with_primary_key
        order by activity_id
    )

select *
from activity_zones_staged
ORDER BY last_updated_date DESC, activity_id, zone_type_index, zone_index
