{{
  config(
    materialized='table'
  )
}}

with trips_data as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    where fare_amount > 0
        and trip_distance > 0
        and payment_type_description in ('Cash', 'Credit Card')
)

SELECT
    service_type,
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90
FROM trips_data
WHERE year = 2020 AND month = 4
ORDER BY service_type
