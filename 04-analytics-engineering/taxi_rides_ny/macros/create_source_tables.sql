{% macro create_source_tables() %}

    {% set sql %}
        CREATE TABLE IF NOT EXISTS {{ source('raw', 'green_tripdata') }} (
            VendorID INT64,
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            store_and_fwd_flag STRING,
            RatecodeID INT64,
            PULocationID INT64,
            DOLocationID INT64,
            passenger_count INT64,
            trip_distance FLOAT64,
            fare_amount FLOAT64,
            extra FLOAT64,
            mta_tax FLOAT64,
            tip_amount FLOAT64,
            tolls_amount FLOAT64,
            ehail_fee FLOAT64,
            improvement_surcharge FLOAT64,
            total_amount FLOAT64,
            payment_type INT64,
            trip_type INT64,
            congestion_surcharge FLOAT64
        );

        CREATE TABLE IF NOT EXISTS {{ source('raw', 'fhv_tripdata') }} (
            dispatching_base_num STRING,
            pickup_datetime TIMESTAMP,
            dropOff_datetime TIMESTAMP,
            PUlocationID INT64,
            DOlocationID INT64,
            SR_Flag INT64,
            Affiliated_base_number STRING
        );
    {% endset %}

    {% do run_query(sql) %}
    {{ print("Source tables created successfully.") }}

{% endmacro %}
