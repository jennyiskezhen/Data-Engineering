{{
    config(
        materialized='view'
    )
}}

with tripdata as (
    select *,
        row_number() over(partition by dispatching_base_num, pickup_datetime
                          order by pulocationid, dropoff_datetime) as rn
    from {{ source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    sr_flag,
    dispatching_base_num,
    affiliated_base_number

from tripdata
where extract(year from pickup_datetime) = 2019

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
