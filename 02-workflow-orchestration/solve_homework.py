import os
import requests
import gzip
import shutil

def download_and_get_info(url, filename):
    print(f"Processing {url}...")
    local_gz = filename + ".gz"
    
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_gz, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return 0, 0
                
    with gzip.open(local_gz, 'rb') as f_in:
        with open(filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            
    size = os.path.getsize(filename)
    
    rows = 0
    with open(filename, 'rb') as f:
        for _ in f:
            rows += 1
            
    data_rows = rows - 1
            
    if os.path.exists(local_gz):
        os.remove(local_gz)
    if os.path.exists(filename):
        os.remove(filename)
    
    # print(f"  Rows: {data_rows}, Size: {size}")
    return data_rows, size

def main():
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    
    # Q3: Yellow 2020 rows & partially Q1
    total_yellow_2020 = 0
    q1_size = 0
    
    print("Computing Yellow 2020...")
    for month in range(1, 13):
        m_str = f"{month:02d}"
        url = f"{base_url}/yellow/yellow_tripdata_2020-{m_str}.csv.gz"
        rows, size = download_and_get_info(url, f"yellow_2020_{m_str}.csv")
        total_yellow_2020 += rows
        if month == 12:
            q1_size = size

    # Q4: Green 2020 rows
    total_green_2020 = 0
    print("Computing Green 2020...")
    for month in range(1, 13):
        m_str = f"{month:02d}"
        url = f"{base_url}/green/green_tripdata_2020-{m_str}.csv.gz"
        rows, size = download_and_get_info(url, f"green_2020_{m_str}.csv")
        total_green_2020 += rows

    # Q5: Yellow 2021-03 rows
    print("Computing Yellow 2021-03...")
    url = f"{base_url}/yellow/yellow_tripdata_2021-03.csv.gz"
    rows_2021_03, _ = download_and_get_info(url, "yellow_2021_03.csv")

    print("\n=== RESULTS ===")
    print(f"Q1: Size of yellow_tripdata_2020-12.csv: {q1_size / 1024 / 1024:.2f} MiB")
    print(f"Q3: Total rows for Yellow 2020: {total_yellow_2020}")
    print(f"Q4: Total rows for Green 2020: {total_green_2020}")
    print(f"Q5: Rows for Yellow 2021-03: {rows_2021_03}")

if __name__ == "__main__":
    main()
