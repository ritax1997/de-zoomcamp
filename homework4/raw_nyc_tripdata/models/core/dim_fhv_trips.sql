{{
  config(
    materialized='table'
  )
}}

with fhv_trips as(
    select *
    from {{ ref('stg_fhv_tripdata')}}
    where dispatching_base_num is not null
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv_trips.dispatching_base_num,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_trips.pickup_datetime,
    fhv_trips.dropOff_datetime,
    fhv_trips.pickup_locationid,
    fhv_trips.dropoff_locationid,
    fhv_trips.sr_flag,
    fhv_trips.affiliated_base_number,
    extract(year from fhv_trips.pickup_datetime) as year,
    extract(month from fhv_trips.pickup_datetime) as month

from fhv_trips
inner join dim_zones as pickup_zone
on fhv_trips.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_trips.dropoff_locationid = dropoff_zone.locationid
