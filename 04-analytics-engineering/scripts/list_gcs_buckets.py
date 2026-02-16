from google.cloud import storage
import os

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'

try:
    client = storage.Client()
    print(f"Listing buckets in project {client.project}:")
    buckets = list(client.list_buckets())
    if buckets:
        for bucket in buckets:
            print(f"- {bucket.name}")
    else:
        print("No buckets found.")
except Exception as e:
    print(f"Error listing buckets: {e}")
