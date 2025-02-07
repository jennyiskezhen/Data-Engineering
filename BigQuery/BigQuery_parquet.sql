-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-jenny.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://ny-taxi-jenny-kestra/yellow_tripdata_2024-*.parquet']
);

-- Create a table from the external table
create or replace table ny-taxi-jenny.zoomcamp.yellow_tripdata_raw as
select *
from ny-taxi-jenny.zoomcamp.external_yellow_tripdata;

-- read data from external table
select count(distinct(PULocationID))
from ny-taxi-jenny.zoomcamp.external_yellow_tripdata;

-- read data from table
select count(distinct(PULocationID))
from ny-taxi-jenny.zoomcamp.yellow_tripdata_raw;

-- read data from table
select PULocationID
from ny-taxi-jenny.zoomcamp.yellow_tripdata_raw;

-- read data from table
select PULocationID, DOLocationID
from ny-taxi-jenny.zoomcamp.yellow_tripdata_raw;

-- count fare amount of 0
select count(fare_amount)
from ny-taxi-jenny.zoomcamp.yellow_tripdata_raw
where fare_amount = 0;

-- partition and filter
create or replace table ny-taxi-jenny.zoomcamp.yellow_tripdata_opt 
partition by 
  Date(tpep_dropoff_datetime)
cluster by 
  VendorID as
select *
from ny-taxi-jenny.zoomcamp.external_yellow_tripdata
;

-- retrieve distinct PUlocationID from raw table
select distinct(VendorID)
from ny-taxi-jenny.zoomcamp.yellow_tripdata_raw 
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';

-- retrive distinct PUlocationID from opt table
select distinct(VendorID)
from ny-taxi-jenny.zoomcamp.yellow_tripdata_opt 
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';

-- retrive count all from opt parquet dataset has 0 byte proccesed
select count(*)
from ny-taxi-jenny.zoomcamp.yellow_tripdata_opt 
;