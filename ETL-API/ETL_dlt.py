# pip install "dlt[duckdb]"
import dlt
import duckdb

# 1. dlt version
print(dlt.__version__)

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# definition of the resource
@dlt.resource(name="rides", write_disposition="append")
def ny_taxi(cursor_date = dlt.sources.incremental("Trip_Pickup_DateTime")):
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

# define new dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline", 
    destination="duckdb", 
    dataset_name="ny_taxi_data")

# run the pipeline with the resources
load_info = pipeline.run(ny_taxi)
print(load_info)

# connect to duckdb
conn = duckdb.connect("ny_taxi_pipeline.duckdb")

# 2. check number of tables
conn.sql('SHOW ALL TABLES').show()

# 3. explore the data
df = pipeline.dataset(dataset_type="default").rides.df()
print(df)

# 4. average trip duration
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)
