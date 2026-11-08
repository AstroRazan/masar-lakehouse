{{ config(materialized='incremental', incremental_strategy='merge',
          unique_key='trip_id', file_format='delta', alias='trips',
          on_schema_change='append_new_columns') }}

with trips as (
    select * from {{ ref('stg_trips') }}
    {% if is_incremental() %}
      where started_at >= (select max(started_at) - interval 3 days from {{ this }})
    {% endif %}
),
drivers as (select * from {{ ref('stg_drivers') }}),
joined as (
    select t.*, d.vehicle_type, d.driver_rating,
        (unix_timestamp(t.ended_at) - unix_timestamp(t.started_at)) / 60.0 as duration_min,
        t.fare_sar / nullif(t.distance_km, 0)                    as fare_per_km,
        hour(t.started_at)                                       as start_hour,
        case when hour(t.started_at) < 6 then 1 else 0 end       as is_night
    from trips t left join drivers d on t.driver_id = d.driver_id
    where t.status = 'completed'
      and t.ended_at > t.started_at
)
select * from joined