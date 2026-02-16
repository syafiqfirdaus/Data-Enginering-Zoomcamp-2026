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
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(cast(pulocationid as numeric) as integer) as pickup_location_id,
        cast(cast(dolocationid as numeric) as integer) as dropoff_location_id,
        cast(cast(sr_flag as numeric) as integer) as sr_flag,
        affiliated_base_number

    from source

)

select * from renamed
-- where dispatching_base_num is not null -- Question 6 requirement: Filter out records where dispatching_base_num IS NULL
where dispatching_base_num is not null
{% if target.name == 'dev' %}
limit 100
{% endif %}
