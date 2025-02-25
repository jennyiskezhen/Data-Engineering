{{
    config(
        materialized='table'
    )
}}

with fact_fhvtrips as (
    select 
        dropoff_datetime,
        pickup_datetime,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration,
        year,
        month,
        pickup_zone,
        dropoff_zone,
    from {{ ref('fact_fhv_trips') }}
)

select 
    year,
    month,
    pickup_zone,
    dropoff_zone,
    max(p90) as p90,
    count(*) as count, 
    RANK() OVER (PARTITION BY pickup_zone ORDER BY max(p90) desc) AS rank
from ( 
    select 
        year,
        month,
        pickup_zone,
        dropoff_zone,
        -- APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] as p90,
        PERCENTILE_CONT(trip_duration, 0.9) OVER(PARTITION BY year, month, pickup_zone, dropoff_zone) AS p90,
        from fact_fhvtrips
        -- UNNEST([trip_duration]) as x
        where year = 2019
            and
            month = 11
            and
            pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
)
group by year, month, pickup_zone, dropoff_zone
order by pickup_zone, p90 desc
