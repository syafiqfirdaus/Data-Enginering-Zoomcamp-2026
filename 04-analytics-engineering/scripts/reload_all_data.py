import requests
import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Config
PROJECT_ID = 'gen-lang-client-0756788832'
DATASET_ID = f"{PROJECT_ID}.trips_data_all"
CREDENTIALS_FILE = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'
DOWNLOAD_DIR = "temp_download"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_FILE
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

client = bigquery.Client()
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

# Schemas
# FHV: SR_Flag as STRING to handle mixed types
fhv_schema = [
    bigquery.SchemaField("dispatching_base_num", "STRING"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("dropOff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("PUlocationID", "INTEGER"),
    bigquery.SchemaField("DOlocationID", "INTEGER"),
    bigquery.SchemaField("SR_Flag", "STRING"), # Relaxed type
    bigquery.SchemaField("Affiliated_base_number", "STRING"),
]

# Green: Standard
green_schema = [
    bigquery.SchemaField("VendorID", "STRING"), # Relaxed
    bigquery.SchemaField("lpep_pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("lpep_dropoff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING"),
    bigquery.SchemaField("RatecodeID", "STRING"), # Relaxed
    bigquery.SchemaField("PULocationID", "STRING"), # Relaxed
    bigquery.SchemaField("DOLocationID", "STRING"), # Relaxed
    bigquery.SchemaField("passenger_count", "STRING"), # Relaxed
    bigquery.SchemaField("trip_distance", "FLOAT"),
    bigquery.SchemaField("fare_amount", "FLOAT"),
    bigquery.SchemaField("extra", "FLOAT"),
    bigquery.SchemaField("mta_tax", "FLOAT"),
    bigquery.SchemaField("tip_amount", "FLOAT"),
    bigquery.SchemaField("tolls_amount", "FLOAT"),
    bigquery.SchemaField("ehail_fee", "STRING"), # Relaxed type
    bigquery.SchemaField("improvement_surcharge", "FLOAT"),
    bigquery.SchemaField("total_amount", "FLOAT"),
    bigquery.SchemaField("payment_type", "STRING"), # Relaxed
    bigquery.SchemaField("trip_type", "STRING"), # Relaxed
    bigquery.SchemaField("congestion_surcharge", "FLOAT"),
]

def drop_table(table_name):
    table_id = f"{DATASET_ID}.{table_name}"
    print(f"Dropping {table_id}...")
    client.delete_table(table_id, not_found_ok=True)

def create_table(table_name, schema):
    table_id = f"{DATASET_ID}.{table_name}"
    print(f"Creating {table_id}...")
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

def load_file_to_bq(file_path, table_id, schema):
    print(f"Loading {file_path} into {table_id}...")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True # Help with schema drift
    )
    try:
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {job.output_rows} rows.")
    except Exception as e:
        print(f"Failed to load {table_id}: {e}")

def process_data(service, year, schema):
    table_name = f"{service}_tripdata"
    table_id = f"{DATASET_ID}.{table_name}"
    
    # Drop and recreate table ONCE per service (at start of script? No, we might run this for multiple years)
    # So we should drop before the loop.
    
    print(f"Processing {service} {year}...")
    
    for month in range(1, 13):
        month_str = f"{month:02d}"
        filename = f"{service}_tripdata_{year}-{month_str}.parquet"
        url = f"{BASE_URL}/{filename}"
        local_path = os.path.join(DOWNLOAD_DIR, filename)
        
        # Check if file already exists (maybe from previous run), skipping download if so?
        # But we want to ensure fresh start if we suspect corruption. 
        # But for speed, let's keep it if it's substantial size.
        if os.path.exists(local_path) and os.path.getsize(local_path) > 1000:
             print(f"File exists: {local_path}, skipping download.")
        else:
            print(f"Downloading {url}...")
            try:
                with requests.get(url, headers=HEADERS, stream=True) as r:
                    r.raise_for_status()
                    with open(local_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
            except Exception as e:
                print(f"Failed download {url}: {e}")
                continue
            
        # Load
        load_file_to_bq(local_path, table_id, schema)
        
        # Clean up
        # os.remove(local_path) # Keep for now in case of retry

# Execution
tables_to_process = [
    ("green", ["2019", "2020"], green_schema),
    ("fhv", ["2019"], fhv_schema)
]

for service, years, schema in tables_to_process:
    drop_table(f"{service}_tripdata")
    create_table(f"{service}_tripdata", schema)
    for year in years:
        process_data(service, year, schema)
