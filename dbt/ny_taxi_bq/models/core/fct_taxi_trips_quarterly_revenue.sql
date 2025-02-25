{{
    config(
        materialized='table'
    )
}}

with fact_tripdata_quarterly_green as (
    select 
        service_type,
        year,
        quarter,
        year_quarter,
        sum(total_amount) as quarter_revenue,
    from {{ ref('fact_trips') }}
    where 
        service_type = 'Green'
        and
        year between 2019 and 2020
    group by service_type, year, quarter, year_quarter
    order by service_type, year, quarter
), 
fact_tripdata_quarterly_yellow as (
    select 
        service_type,
        year,
        quarter,
        year_quarter,
        sum(total_amount) as quarter_revenue,
    from {{ ref('fact_trips') }}
    where 
        service_type = 'Yellow' 
        and
        year between 2019 and 2020
    group by service_type, year, quarter, year_quarter
    order by service_type, year, quarter
)

select
    service_type,
    year,
    quarter,
    year_quarter,
    quarter_revenue,
    lag(quarter_revenue, 4) over (order by year, quarter) as qrr_4_quarters_ago, 
    100 * (quarter_revenue / nullif(lag(quarter_revenue, 4) over (order by year, quarter), 0) - 1) as yoy_qrr_growth
FROM fact_tripdata_quarterly_green 
union all
select
    service_type,
    year,
    quarter,
    year_quarter,
    quarter_revenue,
    lag(quarter_revenue, 4) over (order by year, quarter) as qrr_4_quarters_ago, 
    100 * (quarter_revenue / nullif(lag(quarter_revenue, 4) over (order by year, quarter), 0) - 1) as yoy_qrr_growth
FROM fact_tripdata_quarterly_yellow 
order by service_type, year, quarter