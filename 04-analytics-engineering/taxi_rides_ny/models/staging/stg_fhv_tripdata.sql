{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('raw', 'fhv_tripdata') }}

),

renamed as (

    select
        dispatching_base_num,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,
        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,
        SR_Flag as sr_flag,
        Affiliated_base_number as affiliated_base_number

    from source

)

select * from renamed
-- where dispatching_base_num is not null -- Question 6 requirement: Filter out records where dispatching_base_num IS NULL
where dispatching_base_num is not null
{% if target.name == 'dev' %}
limit 100
{% endif %}
