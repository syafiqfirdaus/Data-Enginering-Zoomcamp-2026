from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'
client = bigquery.Client()
table_id = "gen-lang-client-0756788832.trips_data_all.fhv_tripdata"

print(f"Dropping {table_id}...")
try:
    client.delete_table(table_id, not_found_ok=True)
    print("Table dropped.")
except Exception as e:
    print(f"Error dropping table: {e}")
