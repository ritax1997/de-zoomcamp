{{ config(materialized='table') }}

WITH year_of_q AS (
    SELECT 
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter, 
        CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/Q', EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter,
        SAFE_CAST(service_type AS STRING) AS service_type,  
        total_amount
    FROM {{ ref('fact_trips') }}
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
),
quarter_revenue AS (
    SELECT 
        service_type,
        year_quarter,
        year, 
        quarter,
        SUM(total_amount) AS quarterly_revenue
    FROM year_of_q
    GROUP BY service_type, year_quarter, year, quarter
)

SELECT 
    q1.year, 
    q2.year AS prev_year,
    q1.year_quarter,
    q1.service_type,
    q1.quarterly_revenue,
    q2.quarterly_revenue AS prev_quarterly_revenue,
    ROUND(100 * (q1.quarterly_revenue - q2.quarterly_revenue) / NULLIF(q2.quarterly_revenue, 0), 2) AS yoy_growth
FROM quarter_revenue q1
LEFT JOIN quarter_revenue q2
    ON q1.year = q2.year + 1 
    AND q1.quarter = q2.quarter 
    AND q1.service_type = q2.service_type
ORDER BY q1.service_type, q1.year_quarter, yoy_growth DESC
