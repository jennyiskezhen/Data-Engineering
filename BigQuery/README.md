## BigQuery

#### 1. Create external table 
Load the parquet dataset (<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>) from the past months to GCS (i.e., GCP bucket) using backfill in Kestra. Then create external table using the parquet dataset from GCS

```
CREATE OR REPLACE EXTERNAL TABLE `<project_name>.<bq_dataset>.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://<GCP bucket>/yellow_tripdata_2024-*.parquet']
);
```

#### 2. Estimated data processed on the external table compared to the materialized table
The estimated data processed on the external table is 0 MB, while on the materialized table is 155.12 MB. This is because the external table is not stored in BigQuery and it is a temporary table. Therefore, the amount of data processed can't be determined until the query completes.

#### 3. Columnar database
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns requires reading double the data than querying one column, leading to around double the estimated number of bytes processed.

#### 4. Query the materialized table created from the external table

```
-- Create a table from the external table without partition
create or replace table <project_name>.<bq_dataset>.yellow_tripdata_raw as
select *
from <project_name>.<bq_dataset>.external_yellow_tripdata;

-- count fare amount of 0
select count(fare_amount)
from <project_name>.<bq_dataset>.yellow_tripdata_raw
where fare_amount = 0;
```

#### 5. Best strategy to make an optimized table in BigQuery if the query will always order the results by VendorID and filter based on tpep\_dropoff\_datetime
The table should be partitioned by tpep\_dropoff\_datetime and clustered by VendorID.

```
create or replace table <project_name>.<bq_dataset>.yellow_tripdata_opt 
partition by 
  Date(tpep_dropoff_datetime)
cluster by 
  VendorID as
select *
from <project_name>.<bq_dataset>.external_yellow_tripdata;
```

#### 6. Retrieve the distinct VendorIDs between tpep\_dropoff\_datetime 03/01/2024 and 03/15/2024 (inclusive)

```
select distinct(VendorID)
from <project_name>.<bq_dataset>.yellow_tripdata_opt 
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';
```
The data processed on the non-partitioned table is 310.24 MB while on the partitioned table is 26.84 MB.

#### 7. The storage of the data in the external table
Data in the external table is stored in an external source other than BigQuery. In this case, the data is stored in GCP bucket.

#### 8. Use cluster instead of partition when:
- Partitioning results in a smaller amount of data per partition
- Partitioning results in a large number of partitions beyond the limit
- partitioning results in a lot of modifications 

#### 9. Metadata operations
When using `count(*)`, BigQuery doesn't scan the actual data but rather the metadata (information about the data). Therefore it is free.