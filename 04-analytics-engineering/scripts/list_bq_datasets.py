from google.cloud import bigquery
import os

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'

client = bigquery.Client()
project_id = 'gen-lang-client-0756788832'

print(f"Listing datasets in {project_id}:")
try:
    datasets = list(client.list_datasets(project_id))
    if datasets:
        for dataset in datasets:
            print(f"- {dataset.dataset_id}")
    else:
        print("No datasets found.")
except Exception as e:
    print(f"Error listing datasets: {e}")
