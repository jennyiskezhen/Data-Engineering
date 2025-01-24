## Data Engineering Tools

In this repository, I summarize the data engineering tools that I implemented from joining the data-engineering-zoomcamp by DataTalksClub. The complete zoom camp syllabus can be found here: <https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main>

The datasets come from the NYC Taxi and Limousine Commission (TLC) trip record data, which are public records and can be found here: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

### 1. Containerization (Docker) and Infrastructure as Code (Terraform)

I created a `Postgres` database Docker container and designed a data ingestion script using Python.  After ingesting the dataset to Postgres using **Docker**, I connected Postgres and pgAdmin (a user interface for Postgres) using Docker Compose. In pgAdmin, I explored the NYC taxi ride dataset using SQL.

For cloud storage and cloud computing, I used Google Cloud Platform (GCP). I created resources including `cloud storage bucket (GCS)` and `BigQuery` dataset in GCP using **Terraform**. I also created a VM instance to run docker and terraform.


#### Docker specifics

Create a docker network

```
docker network create pg-network
```

Create a container for Postgres

```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:13
```
Build and ingest the data after specifying the ingesting script in the Dockerfile

```
docker build -t taxi_ingest:v001 .
```
```
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
```
```
docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}
```

### 2. Workflow Orchestration (Kestra)
I extracted and loaded the datasets to `Postgres` and `GCP` using the workflow orchestration platform **Kestra**. The datasets contained real-time record for two types of taxi (yellow and green) for each month. Since the datasets are split by month, I processed and merged the datasets into one table using SQL in Kestra. I implemented scheduling to load the data automatically in each month in the future, and to backfill the missing data from previous months. 

#### 2.1 Extract and Load data to local Postgres using schedule and backfill

Summarized YAML file in Kestra

```
concurrency: limit: 1 #prevent writing multiple datasets to the same staging table#
inputs: taxi type
variables: #set variables with dynamic values
tasks: 
	1. label #labels for each execution 
	2. extract #use wget to download data
	3. If yellow taxi:
		1) create table
		2) create staging-table #for each month
		3) truncate staging-table #remove the previous staging table records
		4) copy to staging-table
		5) add unique id (using md5 hash) and filename to staging-table
		6) merge the staging table to the final main table
	4. If green taxi: #similar to yellow taxi
	5. purge table in Kestra
pluginDefaults: #connect to postgres
triggers: #here are the schedules, backfill can be enabled in the tab
```

#### 2.2 Extract and load data to GCP using schedule and backfill

Summarized YAML file in Kestra

```
inputs: taxi type
variables: #set variables with dynamic values
tasks:
	1. label #labels for each execution 
	2. extract #use wget to download data
	3. upload to GCS
	4. If yellow taxi:
		1) create BigQuery table
		2) create external-table table #get each monthly dataset
		3) create temp-table #table to be merged to the main table, add unique id (using md5 hash) and filename
		4) merge temp-table to table
	5. If green taxi: #similar to yellow taxi
	6. Purge file in Kestra
pluginDefaults: #connect to GCP
triggers: #here are the schedules, backfill can be enabled in the tab
```


