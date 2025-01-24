## Docker, SQL and Terraform

#### 1. Run docker with python image
```
docker run -it --entrypoint=bash python:3.12.8
``` 
```
pip --version
```

#### 2. Docker networking and docker-compose
Inside docker-compose, pgAdmin uses hostname and the container port of the Postgres database to connect to Postgres. Hostname is the service name specified in docker-compose. In port mapping `5433:5432`, the first port `5433` is the port of the local machine, while the second port `5432` is the port of the Postgres container. 

#### 3. Segmentation using SQL
```
select 
	count(*) as count_trips,
case 
	when trip_distance <= 1 then 'Up to 1'
	when trip_distance > 1 and trip_distance <= 3 then '1-3'
	when trip_distance > 3 and trip_distance <= 7 then '3-7'
	when trip_distance > 7 and trip_distance <= 10 then '7-10'
else 
	'over 10'
end as Trip_range
from green_taxi_trips
where cast(lpep_pickup_datetime as date) >= '2019-10-01'
	and cast(lpep_pickup_datetime as date) < '2019-11-01'
	and cast(lpep_dropoff_datetime as date) >= '2019-10-01'
	and cast(lpep_dropoff_datetime as date) < '2019-11-01'
group by Trip_range;
```

#### 4. Aggregation in group by using SQL
```
select
	cast(lpep_pickup_datetime as date) up_date,
	max(trip_distance) max_distance
from green_taxi_trips
group by up_date
order by max_distance desc
limit 20;
```

#### 5. Join two datasets using SQL
```
select
	sum(total_amount) trip_total,
	-- concat(z."Borough",'/',z."Zone")
	"Zone"
from green_taxi_trips trips
join zones z
	on "PULocationID" = "LocationID"
where 
	cast(lpep_pickup_datetime as date) = '2019-10-18'
group by "Zone"
having
	sum(total_amount) > 13000
order by trip_total desc;
```

#### 6. Join the same dataset twice for different usages
```
select
	cast(lpep_pickup_datetime as date) data_pickup,
	tip_amount,
	zup."Zone" Zone_up,
	zoff."Zone" Zone_off
from green_taxi_trips trips
join zones zup
	on trips."PULocationID" = zup."LocationID"
join zones zoff
	on trips."DOLocationID" = zoff."LocationID"
where 
	EXTRACT(YEAR from (cast(lpep_pickup_datetime as date))) = 2019
	and EXTRACT(month from (cast(lpep_pickup_datetime as date))) = 10
	and zup."Zone" = 'East Harlem North'
order by tip_amount desc;
```

#### 7. Terraform Workflow
- `terraform init`: Downloading the provider plugins and setting up backend
- `terraform apply -auto-aprove`: Generating proposed changes and auto-executing the plan
- `terraform destroy`: Remove all resources managed by terraform

