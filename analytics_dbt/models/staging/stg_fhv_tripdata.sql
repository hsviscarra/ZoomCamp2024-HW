{{
    config(
        materialized='view'
    )
}}


with 

source as (

    select * from {{ source('staging', 'fhv_tripdata_02') }}

),

renamed as (

    select
        -- identifiers
        {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as fhv_id,  
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed


-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}


