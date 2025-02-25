{{
    config(
        materialized='table'
    )
}}

with fare_amount_both as (
    select 
        service_type,
        year,
        month,
        fare_amount,
        payment_type_description
    from {{ ref('fact_trips') }}
    where 
        fare_amount > 0
        and
        trip_distance > 0
        and 
        payment_type_description in ('Cash', 'Credit card')
        and
        year between 2019 and 2020
)

select 
    service_type,
    year,
    month,
    max(p97) as p97,
    max(p95) as p95,
    max(p90) as p90,
from (
    select
        service_type,
        year,
        month,
        -- ARRAY_AGG(distinct fare_amount order by fare_amount) as arr,
        PERCENTILE_CONT(fare_amount, 0.97) OVER(PARTITION BY service_type, year, month) AS p97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER(PARTITION BY service_type, year, month) AS p95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER(PARTITION BY service_type, year, month) AS p90,
    from fare_amount_both
)
where year = 2020
      and
      month = 4
group by service_type, year, month
order by service_type, year, month