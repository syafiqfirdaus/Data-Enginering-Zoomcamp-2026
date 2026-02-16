from google.cloud import bigquery
import os

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'

client = bigquery.Client()
project_id = 'gen-lang-client-0756788832'
dataset_id = f"{project_id}.trips_data_all"

# Define tables to load with direct URLs from GitHub Releaases
# Using 2019 and 2020 data as requested.
# Small subsets might be safer for quick loading, but homework requires full processing.
# However, loading large CSVs from HTTP to BigQuery is not directly supported via `load_table_from_uri`.
# `load_table_from_uri` only supports GCS (gs://).
# So we MUST have the files in GCS.

# Since we have no GCS bucket, we must create one and upload data, OR ask user.
# But I can create a bucket easily if I have permissions.
# Let's try to create a bucket first.

bucket_name = f"{project_id}-taxi-data"

from google.cloud import storage
storage_client = storage.Client()

def create_bucket_if_not_exists(bucket_name):
    try:
        bucket = storage_client.bucket(bucket_name)
        if not bucket.exists():
            print(f"Creating bucket {bucket_name}...")
            bucket = storage_client.create_bucket(bucket_name, location="US")
            print(f"Created bucket {bucket.name}")
        else:
            print(f"Bucket {bucket_name} already exists.")
        return bucket
    except Exception as e:
        print(f"Error creating bucket: {e}")
        return None

bucket = create_bucket_if_not_exists(bucket_name)

if not bucket:
    print("Cannot proceed without a GCS bucket.")
    exit(1)

# URLs for data
# Green 2019/2020, Yellow 2019/2020, FHV 2019
# We will use a script to download and upload specific months to save time if we don't need all?
# Homework says "Load the Green and Yellow taxi data for 2019-2020". 
# That is a LOT of data.
# 24 months x 2 colors + 12 months FHV.
# Downloading all might be too heavy for this environment.
# BUT, we can use the `requests` library to stream upload to GCS?
# Or maybe just use the public bucket if we can find one.
# Wait, I can try to access `gs://kestra-de-zoomcamp-bucket` again. Maybe the previous error was something else? "403 Forbidden" would mean permission, "404 Not Found" means bucket doesn't exist.
# The error was "404 Not found: URI gs://dtc_zoomcamp_data". I tried the WRONG bucket.
# I will try `gs://kestra-de-zoomcamp-bucket` in this script.

tables_to_load = [
     {
        "table_name": "green_tripdata_2019",
        "uris": ["gs://kestra-de-zoomcamp-bucket/green_tripdata_2019-*.csv"]
    },
    {
        "table_name": "green_tripdata_2020",
        "uris": ["gs://kestra-de-zoomcamp-bucket/green_tripdata_2020-*.csv"]
    },
    {
        "table_name": "yellow_tripdata_2019",
        "uris": ["gs://kestra-de-zoomcamp-bucket/yellow_tripdata_2019-*.csv"]
    },
    {
        "table_name": "yellow_tripdata_2020",
        "uris": ["gs://kestra-de-zoomcamp-bucket/yellow_tripdata_2020-*.csv"]
    },
    {
        "table_name": "fhv_tripdata_2019",
        "uris": ["gs://kestra-de-zoomcamp-bucket/fhv_tripdata_2019-*.csv"]
    }
]

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

for table in tables_to_load:
    table_id = f"{dataset_id}.{table['table_name']}"
    print(f"Loading {table_id} from {table['uris']}...")
    try:
        load_job = client.load_table_from_uri(
            table['uris'], table_id, job_config=job_config
        )
        load_job.result()
        print(f"Loaded {load_job.output_rows} rows into {table_id}.")
    except Exception as e:
        print(f"Failed to load {table['table_name']}: {e}")
