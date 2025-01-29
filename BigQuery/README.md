## BigQuery

#### 1. Create external table 
Load the parquet dataset (<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>) from the past months to GCS (i.e., GCP bucket) using backfill in Kestra. Then create external table using the parquet dataset from GCS

```
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `<project_name>.<bq_dataset>.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://<GCP bucket>/green_tripdata_2022-*.parquet']
);
```

#### 2. Estimated data processed on the external table compared to the materialized table
The estimated data processed on the external table is 0 MB, while on the materialized table is 6.41 MB. This is because the external table is not stored in BigQuery and it is a temporary table. Therefore, the amount of data processed can't be determined until the query completes.

#### 3. Query the materialized table created from the external table

```
-- Create a table from the external table without partition
create or replace table <project_name>.<bq_dataset>.green_tripdata_raw as
select *
from <project_name>.<bq_dataset>.external_green_tripdata;

-- count fare amount of 0
select count(fare_amount)
from <project_name>.<bq_dataset>.green_tripdata_raw
where fare_amount = 0;
```

#### 4. Best strategy to make an optimized table in BigQuery if the query will always order the results by PUlocationID and filter based on lpep\_pickup\_datetime
The table should be partitioned by lpep\_pickup\_datetime and clustered by PUlocationID.

```
-- partition and filter
create or replace table <project_name>.<bq_dataset>.green_tripdata_opt 
partition by 
  Date(lpep_pickup_datetime)
cluster by 
  PUlocationID as
select *
from <project_name>.<bq_dataset>.external_green_tripdata;
```

#### 5. Retrieve the distinct PULocationID between lpep\_pickup\_datetime 06/01/2022 and 06/30/2022 (inclusive)

```
-- retrive distinct PUlocationID from opt table
select distinct(PUlocationID)
from <project_name>.<bq_dataset>.green_tripdata_opt 
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';
```
The data processed on the non-partitioned table is 12.82 MB while on the partitioned table is 1.12 MB.

#### 6. The storage of the data in the external table
Data in the external table is stored in an external source other than BigQuery. In this case, the data is stored in GCP bucket.

#### 7. Use cluster instead of partition when:
- Partitioning results in a smaller amount of data per partition
- Partitioning results in a large number of partitions beyond the limit
- partitioning results in a lot of modifications 

#### 8. Data processed in the parquet dataset
When using `count(*)` on the parquet dataset, the estimated data processed is 0 bytes, because the parquet file uses compressed and columnar format, which required less amount of data to be scanned.