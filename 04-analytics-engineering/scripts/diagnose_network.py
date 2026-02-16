import requests
import sys

print("Starting network diagnostics...", flush=True)

try:
    print("Checking google.com...", flush=True)
    r = requests.get("https://www.google.com", timeout=5)
    print(f"Google status: {r.status_code}", flush=True)
except Exception as e:
    print(f"Google failed: {e}", flush=True)

try:
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet"
    print(f"Checking taxi data URL: {url}...", flush=True)
    headers = {'User-Agent': 'Mozilla/5.0'}
    # Download first 1KB
    headers['Range'] = 'bytes=0-1024'
    r = requests.get(url, headers=headers, timeout=10)
    print(f"Taxi status: {r.status_code}", flush=True)
    print(f"Content length: {len(r.content)}", flush=True)
except Exception as e:
    print(f"Taxi download failed: {e}", flush=True)

print("Done.", flush=True)
