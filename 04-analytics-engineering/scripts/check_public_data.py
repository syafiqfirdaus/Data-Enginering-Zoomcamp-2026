from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'
client = bigquery.Client()
project_id = 'bigquery-public-data'
dataset_id = 'new_york_taxi_trips'

print(f"Listing tables in {project_id}.{dataset_id}...")
try:
    tables = client.list_tables(f"{project_id}.{dataset_id}")
    for table in tables:
        if "green" in table.table_id or "yellow" in table.table_id or "fhv" in table.table_id:
            print(table.table_id)
except Exception as e:
    print(f"Error: {e}")
