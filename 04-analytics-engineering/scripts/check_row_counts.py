from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'
client = bigquery.Client()
dataset = 'gen-lang-client-0756788832.trips_data_all'

tables = ['green_tripdata', 'yellow_tripdata', 'fhv_tripdata']

for table in tables:
    table_id = f"{dataset}.{table}"
    try:
        row_count = client.get_table(table_id).num_rows
        print(f"{table}: {row_count} rows")
    except Exception as e:
        print(f"{table}: Not found or error ({e})")
