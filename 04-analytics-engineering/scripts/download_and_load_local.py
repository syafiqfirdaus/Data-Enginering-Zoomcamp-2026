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

def load_file_to_bq(file_path, table_id):
    print(f"Loading {file_path} into {table_id}...")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    try:
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()  # Waits for the job to complete.
        print(f"Loaded {job.output_rows} rows into {table_id}.")
    except Exception as e:
        print(f"Failed to load {table_id}: {e}")

def process_data(service, year):
    table_id = f"{DATASET_ID}.{service}_tripdata" # Single table per service? Or {service}_tripdata_{year}?
    # dbt sources refer to 'green_tripdata', 'yellow_tripdata', 'fhv_tripdata'.
    # So we should load everything into these base tables.
    
    # But wait, usually we load into `green_tripdata_2019` etc and then Union.
    # The `sources.yml` I defined has `green_tripdata` as a table.
    # I will load directly into `green_tripdata` etc.
    
    print(f"Processing {service} {year}...")
    
    for month in range(1, 13):
        month_str = f"{month:02d}"
        filename = f"{service}_tripdata_{year}-{month_str}.parquet"
        url = f"{BASE_URL}/{filename}"
        local_path = os.path.join(DOWNLOAD_DIR, filename)
        
        # Download
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
        load_file_to_bq(local_path, table_id)
        
        # Clean up
        if os.path.exists(local_path):
            os.remove(local_path)

# Execution
# Yellow is already mostly loaded (22M rows).
# Green: 2019, 2020
# FHV: 2019

services = [
    ("green", "2019"),
    ("green", "2020"),
    ("fhv", "2019")
]

for service, year in services:
    process_data(service, year)
