from google.cloud import bigquery
import os

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'

client = bigquery.Client()
dataset_id = 'gen-lang-client-0756788832.trips_data_all'

print(f"Listing tables in {dataset_id}:")
tables = client.list_tables(dataset_id) 
found_tables = []
for table in tables:
    print(f"{table.table_id}")
    found_tables.append(table.table_id)

required_tables = ['green_tripdata_2019', 'green_tripdata_2020', 'yellow_tripdata_2019', 'yellow_tripdata_2020', 'fhv_tripdata_2019']

print("\nMissing tables:")
for table in required_tables:
    if not any(t.startswith(table) for t in found_tables):
        print(f"- {table}")
