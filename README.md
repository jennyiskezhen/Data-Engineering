## Data Engineering Tools

In this repository I will be adding data engineering tools that I learned from joining the data-engineering-zoomcamp by DataTalksClub. The complete syllabus can be found here: <https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main>

The datasets I used is the NYC Taxi and Limousine Commission (TLC) trip record data, which are public records and can be found here: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

### Module 1: Containerization (Docker) and Infrastructure as Code (Terraform)

I created a **Postgres** database container and specified a Python ingestion script using Docker. After ingesting the dataset to Postgres, I connected Postgres and pgAdmin (a user interface for Postgres) using docker compose. In pgAdmin, I explored the NYC taxi ride dataset using SQL.

For cloud storage and cloud computing, I used **Google Cloud Platform (GCP)**. I created resources including cloud storage bucket and BigQuery dataset in GCP using Terraform. I also created a VM instance to run docker and terraform.


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
Build and ingest the data after specifying the ingesting script in Dockerfile

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




