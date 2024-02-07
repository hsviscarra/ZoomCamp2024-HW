# ZoomCamp2024-HW

ZOOMCAMP - HOMEWORK 3
https://github.com/hsviscarra/ZoomCamp2024-HW.git


-- Query to create external table
CREATE OR REPLACE EXTERNAL TABLE mage-course.ny_taxi.external_green_taxi
OPTIONS (
  format = 'parquet',
  uris = ['gs://ny_green_taxi_hv/green_tripdata_2022-*.parquet']

);


-- Query to create BQ table
CREATE OR REPLACE TABLE ny_taxi.ny_green_taxi2022 AS
SELECT * FROM ny_taxi.external_green_taxi;

-- What is count of records for the 2022 Green Taxi Data
SELECT COUNT(1) 
FROM ny_taxi.external_green_taxi;


-- Count the distinct number of PULocationIDs for the entire dataset on both tables
SELECT COUNT(DISTINCT(PULocationID))
FROM ny_taxi.external_green_taxi;

SELECT COUNT(DISTINCT(PULocationID))
FROM ny_taxi.ny_green_taxi2022;


-- How many records have a fare_amount of 0?
SELECT COUNT(fare_amount)
FROM ny_taxi.external_green_taxi
WHERE fare_amount=0;


--What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
CREATE OR REPLACE TABLE `ny_taxi.ny_green_taxi2022_partitioned_cluster` 
PARTITION BY DATE(lpep_pickup_datetime) 
CLUSTER BY PUlocationID AS
SELECT * FROM ny_taxi.external_green_taxi;


--Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
SELECT DISTINCT PULocationID
FROM `ny_taxi.ny_green_taxi2022`
WHERE DATE(lpep_pickup_datetime) >= PARSE_DATE('%m/%d/%Y', '06/01/2022')
  AND DATE(lpep_pickup_datetime) <= PARSE_DATE('%m/%d/%Y', '06/30/2022');


SELECT DISTINCT PULocationID
FROM ny_taxi.ny_green_taxi2022_partitioned_cluster
WHERE DATE(lpep_pickup_datetime) >= PARSE_DATE('%m/%d/%Y', '06/01/2022')
  AND DATE(lpep_pickup_datetime) <= PARSE_DATE('%m/%d/%Y', '06/30/2022');

SELECT COUNT(*)
FROM ny_taxi.ny_green_taxi2022_partitioned_cluster
