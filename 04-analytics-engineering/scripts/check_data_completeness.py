from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'
client = bigquery.Client()
dataset_id = 'gen-lang-client-0756788832.trips_data_all'

def check_monthly_counts(table_name, date_col):
    query = f"""
        SELECT 
            EXTRACT(YEAR FROM {date_col}) as year,
            EXTRACT(MONTH FROM {date_col}) as month,
            COUNT(*) as count
        FROM `{dataset_id}.{table_name}`
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    print(f"Checking {table_name}...")
    try:
        results = client.query(query).result()
        for row in results:
            print(f"{row.year}-{row.month:02d}: {row.count}")
    except Exception as e:
        print(f"Error checking {table_name}: {e}")

check_monthly_counts('green_tripdata', 'lpep_pickup_datetime')
check_monthly_counts('yellow_tripdata', 'tpep_pickup_datetime')
check_monthly_counts('fhv_tripdata', 'pickup_datetime')
