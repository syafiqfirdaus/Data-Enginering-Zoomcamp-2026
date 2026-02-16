from google.cloud import bigquery
import os

# Config
PROJECT_ID = 'gen-lang-client-0756788832'
DATASET_ID = f"{PROJECT_ID}.trips_data_all"
CREDENTIALS_FILE = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_FILE
client = bigquery.Client()

def create_table(table_name, schema):
    table_id = f"{DATASET_ID}.{table_name}"
    print(f"Creating {table_id}...")
    try:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        print(f"Created {table_id}")
    except Exception as e:
        print(f"Failed to create {table_id}: {e}")

# Schema validation for Green Taxi
# Based on typical NYC Taxi schema
green_schema = [
    bigquery.SchemaField("VendorID", "INTEGER"),
    bigquery.SchemaField("lpep_pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("lpep_dropoff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING"),
    bigquery.SchemaField("RatecodeID", "INTEGER"),
    bigquery.SchemaField("PULocationID", "INTEGER"),
    bigquery.SchemaField("DOLocationID", "INTEGER"),
    bigquery.SchemaField("passenger_count", "INTEGER"),
    bigquery.SchemaField("trip_distance", "FLOAT"),
    bigquery.SchemaField("fare_amount", "FLOAT"),
    bigquery.SchemaField("extra", "FLOAT"),
    bigquery.SchemaField("mta_tax", "FLOAT"),
    bigquery.SchemaField("tip_amount", "FLOAT"),
    bigquery.SchemaField("tolls_amount", "FLOAT"),
    bigquery.SchemaField("ehail_fee", "FLOAT"),
    bigquery.SchemaField("improvement_surcharge", "FLOAT"),
    bigquery.SchemaField("total_amount", "FLOAT"),
    bigquery.SchemaField("payment_type", "INTEGER"),
    bigquery.SchemaField("trip_type", "INTEGER"),
    bigquery.SchemaField("congestion_surcharge", "FLOAT"),
]

# Schema validation for FHV Taxi
fhv_schema = [
    bigquery.SchemaField("dispatching_base_num", "STRING"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("dropOff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("PUlocationID", "INTEGER"), # Note casing matches staging source usually, but BigQuery is case-insensitive for names but case-preserving.
    bigquery.SchemaField("DOlocationID", "INTEGER"),
    bigquery.SchemaField("SR_Flag", "INTEGER"),
    bigquery.SchemaField("Affiliated_base_number", "STRING"),
]

create_table("green_tripdata", green_schema)
create_table("fhv_tripdata", fhv_schema)
