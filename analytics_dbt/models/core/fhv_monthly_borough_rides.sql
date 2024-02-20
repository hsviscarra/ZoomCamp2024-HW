{{ config(materialized='table') }}

with trips_monthly_rides as (
    select * from {{ ref('fact_fhv_trips') }}
)
    select 
    -- Grouping 
    {{ dbt.date_trunc("month", "pickup_datetime") }} as month, 
    pickup_borough,
    -- Calculations
    count(fhv_id) as total_monthly_rides,
    from trips_monthly_rides
    group by month, pickup_borough
    order by month, pickup_borough