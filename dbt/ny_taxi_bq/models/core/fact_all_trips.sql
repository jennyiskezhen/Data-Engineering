{{
    config(
        materialized='table'
    )
}}

with tripdata as (
    select 
      service_type,
      pickup_datetime,
      dropoff_datetime,
      pickup_borough,
      pickup_zone,
      dropoff_borough,
      dropoff_zone
    from {{ ref('fact_trips') }}
), 
fhv_tripdata as (
    select 
      'fhv' as service_type,
      pickup_datetime,
      dropoff_datetime,
      pickup_borough,
      pickup_zone,
      dropoff_borough,
      dropoff_zone
    from {{ ref('fact_fhv_trips') }}
), 
trips_unioned as (
    select * from tripdata
    union all
    select * from fhv_tripdata
)

select 
    service_type,
    pickup_datetime,
    dropoff_datetime,
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone
from trips_unioned
