{# snapshot snp_activities #}

{#
    config(
      target_database='strava-exploration-v2',
      target_schema='strava_snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='last_updated',
    )
#} 

select * from {{ source('strava_dev', 'activities') }}

{# endsnapshot #} 