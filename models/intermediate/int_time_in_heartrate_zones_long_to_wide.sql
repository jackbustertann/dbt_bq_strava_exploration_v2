with time_in_heartrate_zones_long as (select * from {{ ref("int_time_in_heartrate_zones") }})

select distinct
    activity_id,
    {{
        dbt_utils.pivot(
            "zone_index",
            [1, 2, 3, 4, 5],
            agg="max",
            then_value="moving_time_in_zone_percent",
            prefix="moving_time_in_heartrate_zone_percent_",
            suffix="",
        )
    }}
from time_in_heartrate_zones_long
group by activity_id
