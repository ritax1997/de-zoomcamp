
-- Create external table
CREATE OR REPLACE EXTERNAL TABLE `warehouse-450522.demo_dataset.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2025-450522/yellow_tripdata_2024-*.parquet']
);
-- Create a (regular/materialized) table 
CREATE OR REPLACE TABLE `warehouse-450522.demo_dataset.yellow_tripdata`
AS SELECT * FROM `warehouse-450522.demo_dataset.external_yellow_tripdata`;

SELECT count(*) FROM `warehouse-450522.demo_dataset.external_yellow_tripdata`;

SELECT count(*) FROM `warehouse-450522.demo_dataset.yellow_tripdata`;
-- Q2
-- Query for External Table
SELECT COUNT(DISTINCT PULocationID) as distinct_locations 
FROM `warehouse-450522.demo_dataset.external_yellow_tripdata`;
-- Query for Regular Table
SELECT COUNT(DISTINCT(PULocationID)) as distinct_locations
FROM `warehouse-450522.demo_dataset.yellow_tripdata`;

-- Q3
-- Retrieve the PULocationID
SELECT
  PULocationID  
FROM `warehouse-450522.demo_dataset.yellow_tripdata`;
-- Retrieve the PULocationID and DOLocationID
SELECT
  PULocationID,
  DOLocationID
FROM `warehouse-450522.demo_dataset.yellow_tripdata`;

-- Q4
SELECT COUNT(*)
FROM `warehouse-450522.demo_dataset.external_yellow_tripdata`
WHERE fare_amount = 0.0;

-- Q5
-- CREATE NON-PARTITION TABLE
CREATE OR REPLACE TABLE `warehouse-450522.demo_dataset.nonpartitioned_yellow_tripdata`
AS SELECT * FROM `warehouse-450522.demo_dataset.yellow_tripdata`;

-- CREATE PARTITION TABLE
CREATE OR REPLACE TABLE `warehouse-450522.demo_dataset.yellow_tripdata_partitioned`
PARTITION BY
  DATE(tpep_dropoff_datetime) AS
SELECT * FROM `warehouse-450522.demo_dataset.yellow_tripdata`;

-- CREATE PARTITION WITH CLUSTERED
CREATE OR REPLACE TABLE `warehouse-450522.demo_dataset.yellow_tripdata_partitioned_clustered`
PARTITION BY
  DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `warehouse-450522.demo_dataset.yellow_tripdata`;

-- Q6 
SELECT DISTINCT(VendorID)
FROM `warehouse-450522.demo_dataset.yellow_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID)
FROM `warehouse-450522.demo_dataset.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';