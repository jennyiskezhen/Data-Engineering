## Spark

### 1. Spark Internal

I practiced the following methods using PySpark:

```
# create a spark cluster
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
    
spark.conf.set("spark.sql.session.timeZone", "Europe/Berlin")
    
# read raw data with schema 
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .parquet('yellow_tripdata_2024-10.parquet')

# save processed data by partition 
df \
.repartition(4) \
.write.parquet(f'data/pq/yellow/2024/10/', mode='overwrite')

df_yellow = spark.read.parquet('data/pq/yellow/2024/10')

# create temporary view table
df_look.createOrReplaceTempView('lookup_data')

# sql using PySpark
df_lookup = spark.sql("select LocationID, Zone from lookup_data")

# use the join method
df_result = df_yellow.join(df_lookup, df_yellow.PULocationID == df_lookup.LocationID)
```

### 2. Start a local spark cluster 
Run the read.parquet file using Python 

```
python 04_spark_sql_manual.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020
```
Specify the master outside of the python script using `spark-submit`

```
URL="spark://de-zoomcamp.europe-west1-b.c.ny-taxi-jenny.internal:7077"

spark-submit \
    --master="${URL}" \
    04_spark_sql_manual.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021
```

### 3. Start a spark cluster in the cloud (GCP)

Submit job to local storage

```
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west3 \
    gs://ny-taxi-jenny-kestra/code/04_spark_sql_manual.py \
    --  \
        --input_green=gs://ny-taxi-jenny-kestra/pq/green/2021/*/ \
        --input_yellow=gs://ny-taxi-jenny-kestra/pq/yellow/2021/*/  \
        --output=gs://ny-taxi-jenny-kestra/report-2021
```

Submit job to BigQuery

```
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west3 \
    --jars=gs://spark-lib/bigquery/spark-3.4-bigquery-0.42.0.jar \
    gs://ny-taxi-jenny-kestra/code/04_spark_sql_big_query.py \
    --  \
        --input_green=gs://ny-taxi-jenny-kestra/pq/green/2020/*/ \
        --input_yellow=gs://ny-taxi-jenny-kestra/pq/yellow/2020/*/  \
        --output=zoomcamp.reports-2020
```
