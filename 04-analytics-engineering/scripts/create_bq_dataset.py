from google.cloud import bigquery
import os

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'

client = bigquery.Client()
project_id = 'gen-lang-client-0756788832'
dataset_id = f"{project_id}.trips_data_all"

# Create Dataset
print(f"Creating dataset {dataset_id}...")
try:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print(f"Created dataset {client.project}.{dataset.dataset_id}")
except Exception as e:
    print(f"Error creating dataset: {e}")

# Run Query
print("\nRunning test query 'SELECT 1'...")
try:
    query_job = client.query("SELECT 1")
    results = query_job.result()
    for row in results:
        print(f"Query result: {row[0]}")
except Exception as e:
    print(f"Error running query: {e}")
