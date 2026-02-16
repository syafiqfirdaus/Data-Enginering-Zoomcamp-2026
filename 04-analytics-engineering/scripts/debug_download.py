import requests
import os

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet"
output_file = "green_tripdata_2019-01.parquet"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

print(f"Downloading {url}...")
try:
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()
    with open(output_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded {output_file} successfully.")
except Exception as e:
    print(f"Failed to download: {e}")
