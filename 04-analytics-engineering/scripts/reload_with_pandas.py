import requests
import os
import pandas as pd
from google.cloud import bigquery

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

# Schemas for BigQuery (Target)
# We will cast DataFrame to match these types implicitly
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

yellow_schema = [
    bigquery.SchemaField("VendorID", "INTEGER"),
    bigquery.SchemaField("tpep_pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("tpep_dropoff_datetime", "TIMESTAMP"),
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
    bigquery.SchemaField("improvement_surcharge", "FLOAT"),
    bigquery.SchemaField("total_amount", "FLOAT"),
    bigquery.SchemaField("payment_type", "INTEGER"),
    bigquery.SchemaField("congestion_surcharge", "FLOAT"),
    bigquery.SchemaField("airport_fee", "FLOAT"),
]

fhv_schema = [
    bigquery.SchemaField("dispatching_base_num", "STRING"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("dropOff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("PUlocationID", "INTEGER"),
    bigquery.SchemaField("DOlocationID", "INTEGER"),
    bigquery.SchemaField("SR_Flag", "INTEGER"), # Normalize to Int (or String if really mixed)
    bigquery.SchemaField("Affiliated_base_number", "STRING"),
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

def normalize_green_df(df):
    # Ensure numeric columns are numeric (float handles nulls)
    numeric_cols = ['VendorID', 'RatecodeID', 'PULocationID', 'DOLocationID', 'passenger_count', 'payment_type', 'trip_type', 'ehail_fee']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Store and fwd flag
    if 'store_and_fwd_flag' in df.columns:
        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype(str).replace('nan', None).replace('None', None)

    # Convert datetimes
    if 'lpep_pickup_datetime' in df.columns:
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    if 'lpep_dropoff_datetime' in df.columns:
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        
    return df

def normalize_yellow_df(df):
    # Ensure numeric columns are numeric
    numeric_cols = ['VendorID', 'RatecodeID', 'PULocationID', 'DOLocationID', 'passenger_count', 'payment_type', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Store and fwd flag
    if 'store_and_fwd_flag' in df.columns:
        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype(str).replace('nan', None).replace('None', None)

    # Convert datetimes
    if 'tpep_pickup_datetime' in df.columns:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    if 'tpep_dropoff_datetime' in df.columns:
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        
    return df

def normalize_fhv_df(df):
    if 'SR_Flag' in df.columns:
        df['SR_Flag'] = pd.to_numeric(df['SR_Flag'], errors='coerce')
    if 'PUlocationID' in df.columns:
        df['PUlocationID'] = pd.to_numeric(df['PUlocationID'], errors='coerce')
    if 'DOlocationID' in df.columns:
        df['DOlocationID'] = pd.to_numeric(df['DOlocationID'], errors='coerce')
        
    # Datetimes
    if 'pickup_datetime' in df.columns:
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    if 'dropOff_datetime' in df.columns:
        df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])

    return df

def load_df_to_bq(df, table_id, schema):
    print(f"Loading DataFrame ({len(df)} rows) into {table_id}...")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {len(df)} rows.")
    except Exception as e:
        print(f"Failed to load: {e}")

def process_data(service, year, schema):
    table_name = f"{service}_tripdata"
    table_id = f"{DATASET_ID}.{table_name}"
    
    print(f"Processing {service} {year}...")
    
    for month in range(1, 13):
        month_str = f"{month:02d}"
        filename = f"{service}_tripdata_{year}-{month_str}.parquet"
        url = f"{BASE_URL}/{filename}"
        local_path = os.path.join(DOWNLOAD_DIR, filename)
        
        # Download if missing or too small (e.g. < 1MB)
        if not (os.path.exists(local_path) and os.path.getsize(local_path) > 1_000_000):
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
        else:
            print(f"Using local {filename}")
            
        # Read and Normalize
        try:
            df = pd.read_parquet(local_path)
            if service == 'green':
                df = normalize_green_df(df)
            elif service == 'yellow':
                df = normalize_yellow_df(df)
            elif service == 'fhv':
                df = normalize_fhv_df(df)
            
            # Load
            load_df_to_bq(df, table_id, schema)
        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Execution
tables_to_process = [
    ("green", ["2019", "2020"], green_schema),
    ("yellow", ["2019", "2020"], yellow_schema),
    ("fhv", ["2019"], fhv_schema)
]

for service, years, schema in tables_to_process:
    drop_table(f"{service}_tripdata")
    create_table(f"{service}_tripdata", schema)
    for year in years:
        process_data(service, year, schema)
