-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-jenny.zoomcamp.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://ny-taxi-jenny-kestra/green_tripdata_2022-*.parquet']
);

-- Create a table from the external table
create or replace table ny-taxi-jenny.zoomcamp.green_tripdata_raw as
select *
from ny-taxi-jenny.zoomcamp.external_green_tripdata;

-- read data from external table
select count(distinct(PULocationID))
from ny-taxi-jenny.zoomcamp.external_green_tripdata;

-- read data from table
select count(distinct(PULocationID))
from ny-taxi-jenny.zoomcamp.green_tripdata_raw;

-- count fare amount of 0
select count(fare_amount)
from ny-taxi-jenny.zoomcamp.green_tripdata_raw
where fare_amount = 0;

-- partition and filter
create or replace table ny-taxi-jenny.zoomcamp.green_tripdata_opt 
partition by 
  Date(lpep_pickup_datetime)
cluster by 
  PUlocationID as
select *
from ny-taxi-jenny.zoomcamp.external_green_tripdata
;

-- retrive distinct PUlocationID from raw table
select distinct(PUlocationID)
from ny-taxi-jenny.zoomcamp.green_tripdata_raw 
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';

-- retrive distinct PUlocationID from opt table
select distinct(PUlocationID)
from ny-taxi-jenny.zoomcamp.green_tripdata_opt 
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';

-- retrive count all from opt parquet dataset has 0 byte proccesed
select count(*)
from ny-taxi-jenny.zoomcamp.green_tripdata_opt 
;