from google.cloud import bigquery
import os

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'

client = bigquery.Client()
project_id = 'gen-lang-client-0756788832'
dataset_id = f"{project_id}.trips_data_all"

# Define tables to load
tables_to_load = [
    {
        "table_name": "green_tripdata_2019",
        "uri": "gs://kestra-de-zoomcamp-bucket/green_tripdata_2019-*.csv" 
    },
    {
        "table_name": "green_tripdata_2020",
        "uri": "gs://kestra-de-zoomcamp-bucket/green_tripdata_2020-*.csv"
    },
    {
        "table_name": "yellow_tripdata_2019",
        "uri": "gs://kestra-de-zoomcamp-bucket/yellow_tripdata_2019-*.csv"
    },
    {
        "table_name": "yellow_tripdata_2020",
        "uri": "gs://kestra-de-zoomcamp-bucket/yellow_tripdata_2020-*.csv"
    },
    {
        "table_name": "fhv_tripdata_2019",
        "uri": "gs://kestra-de-zoomcamp-bucket/fhv_tripdata_2019-*.csv"
    }
]

# Update URIs to point to correct public bucket location if needed. 
# Using data from module 3 or direct links.
# Actually, the most reliable way for this homework is likely loading from the provided public links or user's bucket if they have it.
# Let's try loading from the standard zoomcamp bucket or a known public one.
# Re-checking the homework instructions, it says "Load the Green and Yellow taxi data for 2019-2020".
# They are usually available as parquet or csv.
# Quickest way: Create external tables or load natively. 
# Let's try creating tables from URIs.

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

for table in tables_to_load:
    table_id = f"{dataset_id}.{table['table_name']}"
    print(f"Loading {table_id} from {table['uri']}...")
    
    # We need to be careful with URIs. 
    # If the user doesn't have these in their bucket, we might need to use the public one.
    # Public bucket for 2026/2025: gs://kestra-de-zoomcamp-bucket/ 
    # Let's assume the user has data or we use public. 
    # Attempting to load from 'gs://kestra-de-zoomcamp-bucket/' which was used in previous modules or similar.
    # If this fails, we will need to ask user or use alternate method.
    
    # However, 'gs://kestra-de-zoomcamp-bucket' might not be readable by the service account if it's not public or the SA doesn't have permissions.
    # Valid public bucket often used: gs://. 
    # Let's try `gs:// Mage-Zoomcamp/` or similar? No.
    # Let's use the explicit links from the course if possible.
    # For now, I'll try a generic load pattern. If it fails, I'll update.
    
    # Actually, better to check if we can use external tables to save storage/time? 
    # Homework says "Load ... into your warehouse". 
    # I will use the URIs I typically know: 
    # Green: gs://dtc_zoomcamp_data/data/green/green_tripdata_2019-*.csv.gz
    # Yellow: gs://dtc_zoomcamp_data/data/yellow/yellow_tripdata_2019-*.csv.gz
    
    # Updated URIs based on common course data locations:
    if "green" in table['table_name']:
         year = table['table_name'].split('_')[-1]
         uri = f"gs://dtc_zoomcamp_data/data/green/green_tripdata_{year}-*.csv.gz"
    elif "yellow" in table['table_name']:
         year = table['table_name'].split('_')[-1]
         uri = f"gs://dtc_zoomcamp_data/data/yellow/yellow_tripdata_{year}-*.csv.gz"
    elif "fhv" in table['table_name']:
         uri = f"gs://dtc_zoomcamp_data/data/fhv/fhv_tripdata_2019-*.csv.gz"

    try:
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        load_job.result()  # Waits for the job to complete.
        print(f"Loaded {load_job.output_rows} rows into {table_id}.")
    except Exception as e:
        print(f"Failed to load {table['table_name']}: {e}")

