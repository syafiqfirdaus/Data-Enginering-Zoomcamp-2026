import os
import urllib.request
from google.cloud import storage
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor
import time

# Config
PROJECT_ID = 'gen-lang-client-0756788832'
BUCKET_NAME = "dezoomcamp_hw3_2025_muhds" # Using user's bucket from previous module
DATASET_ID = f"{PROJECT_ID}.trips_data_all"
CREDENTIALS_FILE = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'
DOWNLOAD_DIR = "temp_data"

# URLs
# Using Parquet for efficiency
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_FILE
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

storage_client = storage.Client()
bq_client = bigquery.Client()

def create_bucket_if_not_exists(bucket_name):
    try:
        bucket = storage_client.bucket(bucket_name)
        if not bucket.exists():
            print(f"Creating bucket {bucket_name}...")
            bucket = storage_client.create_bucket(bucket_name, location="US")
        return bucket
    except Exception as e:
        print(f"Error accessing/creating bucket: {e}")
        return None

bucket = create_bucket_if_not_exists(BUCKET_NAME)
if not bucket:
    exit(1)

def process_month(service, year, month):
    filename = f"{service}_tripdata_{year}-{month}.parquet"
    url = f"{BASE_URL}/{filename}"
    local_path = os.path.join(DOWNLOAD_DIR, filename)
    blob_name = f"{service}/{filename}" # Organize in folders
    
    # 1. Download
    if not os.path.exists(local_path):
        print(f"Downloading {filename}...")
        try:
            urllib.request.urlretrieve(url, local_path)
        except Exception as e:
            print(f"Failed to download {url}: {e}")
            return None
    
    # 2. Upload to GCS
    blob = bucket.blob(blob_name)
    if not blob.exists():
        print(f"Uploading {blob_name}...")
        try:
            blob.upload_from_filename(local_path)
            print(f"Uploaded {blob_name}")
        except Exception as e:
            print(f"Failed to upload {blob_name}: {e}")
            return None
    else:
        print(f"File {blob_name} already exists in GCS. Skipping upload.")

    # 3. Remove local file to save space
    if os.path.exists(local_path):
        os.remove(local_path)
        
    return f"gs://{BUCKET_NAME}/{blob_name}"

def load_to_bq(service, year, uris):
    table_id = f"{DATASET_ID}.{service}_tripdata_{year}"
    print(f"Loading {table_id} from {len(uris)} files...")
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Schema evolution might be needed if columns differ slightly, but usually Parquet handles it.
    )
    
    try:
        load_job = bq_client.load_table_from_uri(
            uris, table_id, job_config=job_config
        )
        load_job.result()
        print(f"Successfully loaded {load_job.output_rows} rows into {table_id}")
    except Exception as e:
        print(f"Failed to load {table_id}: {e}")

# Main execution
services_years = [
    ("green", "2019"),
    ("green", "2020"),
    ("yellow", "2019"),
    ("yellow", "2020"),
    ("fhv", "2019")
]

for service, year in services_years:
    months = [f"{i:02d}" for i in range(1, 13)]
    
    # FHV 2019 might be CSV.gz in some contexts, but Parquet should exist on Cloudfront. 
    # If not, urlretrieve will fail. We'll see.
    
    # Download and Upload
    gcs_uris = []
    # Sequential processing to avoid disk fill
    for month in months:
        uri = process_month(service, year, month)
        if uri:
            gcs_uris.append(uri)
            
    # Load to BQ (one table per year)
    # The staging models usually UNION them later, or we load into 'green_tripdata' partitioned by ingestion time?
    # dbt sources refer to 'green_tripdata'. If we have 'green_tripdata_2019' and '2020', dbt typically selects from ONE table or unions them.
    # The 'sources.yml' defines ONE table 'green_tripdata'. 
    # This implies we should load ALL data into ONE table 'green_tripdata'.
    # I will aggregate URIs and load into specific year tables first, then maybe combine?
    # Or just load all into 'green_tripdata'?
    # 'write_disposition=WRITE_TRUNCATE' would wipe it.
    # 'WRITE_APPEND' is better if we loop.
    # BUT, schema differences between years might cause issues.
    # I will load into 'green_tripdata' and 'yellow_tripdata' directly, appending.
    
    # Wait, 'sources.yml' has ONE table 'green_tripdata'.
    # So I should load all 2019+2020 into `green_tripdata`.
    pass

# Redefine loading strategy
final_load_map = {
    "green_tripdata": [],
    "yellow_tripdata": [],
    "fhv_tripdata": [] # FHV 2019 only
}

# Collect all URIs first? No, we need to upload first.
# Rerun loop with collection
for service, year in services_years:
    for month in [f"{i:02d}" for i in range(1, 13)]:
         uri = f"gs://{BUCKET_NAME}/{service}/{service}_tripdata_{year}-{month}.parquet"
         # We assume process_month succeeded or file exists.
         # Actually we should use the return value.
         # For simplicity in this generated script, I'll rely on the previous loop to have populated GCS.
         # But wait, I didn't actually run the loop in the "Pass" block above.
         pass

# Real execution block
for service, year in services_years:
    print(f"Processing {service} {year}...")
    year_uris = []
    months = [f"{i:02d}" for i in range(1, 13)]
    for month in months:
        uri = process_month(service, year, month)
        if uri:
            year_uris.append(uri)
            
    # Register for final load
    if service == "fhv":
        final_load_map["fhv_tripdata"].extend(year_uris) # 2019 only per request
    else:
        final_load_map[f"{service}_tripdata"].extend(year_uris)

# Load into BQ
for table_name, uris in final_load_map.items():
    if not uris:
        continue
        
    table_id = f"{DATASET_ID}.{table_name}"
    print(f"Loading {len(uris)} files into {table_id}...")
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Reset table 
    )
    
    try:
        load_job = bq_client.load_table_from_uri(
            uris, table_id, job_config=job_config
        )
        load_job.result()
        print(f"Loaded {table_id}")
    except Exception as e:
        print(f"Failed to load {table_id}: {e}")

