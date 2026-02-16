from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'
client = bigquery.Client()
project_id = 'gen-lang-client-0756788832'
dataset_id = 'trips_data_all'
table_id = f"{project_id}.{dataset_id}.fct_monthly_zone_revenue"

query = f"""
    SELECT 
        service_type,
        EXTRACT(YEAR FROM revenue_month) as year,
        COUNT(*) as row_count
    FROM `{table_id}`
    GROUP BY 1, 2
    ORDER BY 1, 2
"""

print("Analyzing fct_monthly_zone_revenue...")
try:
    query_job = client.query(query)
    results = query_job.result()
    
    print(f"{'Service':<10} | {'Year':<5} | {'Count':<10}")
    print("-" * 30)
    for row in results:
        print(f"{row.service_type:<10} | {row.year:<5} | {row.row_count:<10}")
        
except Exception as e:
    print(f"Error: {e}")
