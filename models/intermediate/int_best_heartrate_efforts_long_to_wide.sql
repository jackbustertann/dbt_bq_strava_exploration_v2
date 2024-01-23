with best_heartrate_efforts_long as (select * from {{ ref("int_best_heartrate_efforts") }})

select distinct
    activity_id,
    {{
        dbt_utils.pivot(
            "effort_duration_s",
            ["15s", "60s", "300s", "600s", "1200s"],
            agg="max",
            then_value="best_effort",
            prefix="best_heartrate_bpm_",
            suffix="",
        )
    }}
from best_heartrate_efforts_long
group by activity_id
