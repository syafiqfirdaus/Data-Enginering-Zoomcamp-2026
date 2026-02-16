from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'c:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\03-data-warehouse\gcs.json'
client = bigquery.Client()
project_id = 'gen-lang-client-0756788832'
dataset_id = 'trips_data_all'

# Check stg_fhv_tripdata count
query_fhv = f"""
    SELECT COUNT(*) as count 
    FROM `{project_id}.{dataset_id}.stg_fhv_tripdata`
"""

# Check fct_monthly_zone_revenue for Green 2020
query_green_2020 = f"""
    SELECT pickup_zone, sum(revenue_monthly_total_amount) as annual_revenue
    FROM `{project_id}.{dataset_id}.fct_monthly_zone_revenue`
    WHERE service_type = 'Green' 
      AND EXTRACT(YEAR FROM revenue_month) = 2020
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 3
"""

# Check fct_monthly_zone_revenue for Green Oct 2019
query_green_oct_2019 = f"""
    SELECT sum(total_monthly_trips) as october_trips
    FROM `{project_id}.{dataset_id}.fct_monthly_zone_revenue`
    WHERE service_type = 'Green' 
      AND EXTRACT(YEAR FROM revenue_month) = 2019 
      AND EXTRACT(MONTH FROM revenue_month) = 10
"""

# Check Q3 count (Total)
query_q3_total = f"""
    SELECT COUNT(*) as count 
    FROM `{project_id}.{dataset_id}.fct_monthly_zone_revenue`
"""

# Check Q3 count (2019-2020 only)
query_q3_valid = f"""
    SELECT COUNT(*) as count 
    FROM `{project_id}.{dataset_id}.fct_monthly_zone_revenue`
    WHERE EXTRACT(YEAR FROM revenue_month) BETWEEN 2019 AND 2020
"""

# Check Q3 breakdown
query_q3_breakdown = f"""
    SELECT service_type, EXTRACT(YEAR FROM revenue_month) as year, count(*) as count
    FROM `{project_id}.{dataset_id}.fct_monthly_zone_revenue`
    GROUP BY 1, 2
    ORDER BY 1, 2
"""

def run_query(query, label):
    print(f"\n--- {label} ---")
    try:
        results = client.query(query).result()
        for row in results:
            print(dict(row))
    except Exception as e:
        print(f"Error: {e}")

run_query(query_fhv, "FHV Staging Count")
run_query(query_green_2020, "Green 2020 Best Zones")
run_query(query_green_oct_2019, "Green Oct 2019 Trips")
run_query(query_q3_total, "Q3 Total Count")
run_query(query_q3_valid, "Q3 Valid (2019-2020) Count")
run_query(query_q3_breakdown, "Q3 Breakdown")
