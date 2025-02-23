{{
  config(
    materialized='table'
  )
}}

with filtered_trips as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    where fare_amount > 0
        and trip_distance > 0
        and payment_type_description in ('Cash', 'Credit Card')
),

percentiles as (
    select
        service_type,
        year,
        month,
        -- BigQuery uses PERCENTILE_CONT without WITHIN GROUP
        PERCENTILE_CONT(fare_amount, 0.97) over (partition by service_type, year, month) as p97,
        PERCENTILE_CONT(fare_amount, 0.95) over (partition by service_type, year, month) as p95,
        PERCENTILE_CONT(fare_amount, 0.90) over (partition by service_type, year, month) as p90
    from filtered_trips
)

-- Remove duplicates since window functions will repeat values
select distinct * from percentiles
order by service_type, year, month