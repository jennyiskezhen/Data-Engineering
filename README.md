## Data Engineering Tools

In this repository, I summarize the data engineering tools that I implemented from joining the data-engineering-zoomcamp by DataTalksClub. The complete zoomcamp syllabus can be found here: <https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main>

The datasets come from the NYC Taxi and Limousine Commission (TLC) trip record data, which are public records that can be found here: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

### 1. Containerization (Docker) and Infrastructure as Code (Terraform)

I created a `Postgres` database Docker container and designed a data ingestion script using Python.  After ingesting the dataset to Postgres using **Docker**, I connected Postgres and pgAdmin (a graphical user interface for Postgres) using Docker Compose. In pgAdmin, I explored the NYC taxi ride dataset using SQL.

I used Google Cloud Platform (GCP) for cloud storage and cloud computing. I created resources including `cloud storage bucket (GCS)` and `BigQuery dataset` in GCP using **Terraform**. I also created a VM instance to run docker and terraform.


#### 1.1 Docker

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

#### 1.2 Terraform
- `terraform init`: Downloading the provider plugins and setting up backend
- `terraform apply -auto-aprove`: Generating proposed changes and auto-executing the plan
- `terraform destroy`: Remove all resources managed by terraform

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
### 3. BigQuery - Best practices, ML model and model deployment
BigQuery can be used to query the dataset, build and apply a machine learning model on the dataset.

#### 3.1 Create table with partition and cluster

**Partition** is mostly used for timestamp column, and for filtering/aggregation on one  column. It can reduce BigQuery cost given more contained processing.<br>

**Cluster** is used for column with a wider range of schema. It is often used for ordering and sorting. The specified column determines the sort order of the data.

#### 3.2 Machine learning in BigQuery
```
1. select the features and label from raw table
2. create a table with appropriate schema for ML from the selected features 
3. create the model (i.e., CREATE OR REPLACE MODEL '<project_id>.<bg_dataset>.<model_name>'): 
	- options:
	  model_type = 
	  input_label_cols = 
	  data_split_method = ‘auto_split’ as
	  select * from <table>
4. check the features (i.e., ML.FEATURE_INFO(MODEL '<project_id>.<bg_dataset>.<model_name>'))
4. evaluate the model (i.e., ML.EVALUATE(MODEL '<project_id>.<bg_dataset>.<model_name>'))
5. predict the model (i.e., ML.PREDICT(MODEL '<project_id>.<bg_dataset>.<model_name>'))
6. Predict and explain the model: shows the top_k_features (i.e., ML.EXPLAIN_PREDICT(MODEL '<project_id>.<bg_dataset>.<model_name>')) 
Also supports hyper parameter tuning 
```
#### 3.3 Model deployment using the TensorFlow Serving library and make a prediction using HTTP request
Log in to GCP

```
gcloud auth login
```
Export the model to GCS from BigQuery

```
bq --project_id <project_id> \
extract -m <bq_dataset>.<model_name> gs://<bucket>/<model name> 
```
Create a model folder in `/tmp`

```
mkdir /tmp/model
```
Copy model from GCS to `/tmp` using the `gsutil` tool to access GCS. The `gsutil` tool is a legacy Cloud Storage CLI. GCS is mapped to the `/tmp` directory first

```
gsutil cp -r gs://<bucket>/<model_name> /tmp/model
```

Create specified local directory for the model, version 1

```
mkdir -p serving_dir/<model_name>/1
```
Copy the model from `/tmp` to specified local directory

```
cp -r /tmp/model/<model name>/* $(pwd)/serving_dir/<model_name>/1
```

Get the serving library using docker

```
docker pull tensorflow/serving
```
Create and run a docker container of the model using local server

```
docker run -p 8501:8501 \
--mount type=bind,source=$(pwd)/serving_dir/<model_name>,target=/models/<model_name> \
-e MODEL_NAME=<model_name> \
-t tensorflow/serving
```
Use HTTP GET request method to get the model information

```
curl -X GET http://localhost:8501/v1/models/<model_name>
```

Use HTTP POST request method to request that the server accepts the data and make a prediction

```
curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' \
-X POST http://localhost:8501/v1/models/<model_name>:predict
```

### 4. Data Analytics Engineering
I first set up the dbt environment by connecting to the BigQuery data Warehouse and linking to my GitHub repository.

####  4.1 dbt development

**Background set-up**:

- Specify a macro file that can be applied to all models
- Install package `dbt_utils` to generate unique id using md5 hash and package `codegen` to generate a template model automatically 
- Create a variable in the model to limit number of rows processed

**dbt Models**:

- Models for green and yellow type taxi data staging table, respectively
- a model for pickup and drop-off location lookup table
- a model to join the above three tables

**Add tests to ensure data quality**:

- id is unique and not_null
- join table has the same join_id from separate tables
- only has the accepted values

####  4.2 dbt deployment
- Added a production environment and scheduled a recurring job
- In the production environment, I also created a new job to enable CI/CD on pull requests to allow automatic deployment while updating the changes

####  4.3 Data visualization using Google Looker Studio
I loaded the data source created by using dbt into the Looker Studio. Then I created graphics to show:

- controls to select record period and taxi service type
- pie chart of service type distribution
- time-series of taxi ride counts by different service types
- bar chart of monthly taxi ride counts by different service types
- table showing the taxi ride counts starting at different locations
