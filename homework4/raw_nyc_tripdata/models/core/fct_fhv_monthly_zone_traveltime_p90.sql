{{
  config(
    materialized='table'
  )
}}

with fhv_data as(
    select *,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref("dim_fhv_trips")}}

),
percentile as(
    select *,
    PERCENTILE_CONT(trip_duration, 0.9) over(partition by pickup_locationid, dropOff_locationid) as p90

    from fhv_data
    where pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East') and month = 11

),
rk as(
    select *,
    dense_rank() over(partition by pickup_zone order by p90 desc) as rn
    from percentile

)
select pickup_zone, dropoff_zone 
from rk
where rn = 2
group by 1, 2

    









