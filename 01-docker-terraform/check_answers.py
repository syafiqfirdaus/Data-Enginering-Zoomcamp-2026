
import pandas as pd
from sqlalchemy import create_engine, text

# Database connection parameters
# Matching docker-compose.yaml
user = 'postgres'
password = 'postgres'
host = 'localhost'
port = '5433'
db = 'ny_taxi'

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

def run_query(query):
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return result.fetchall()

print("--- Question 3 ---")
# Count short trips (<= 1 mile) in Nov 2025
# 2025-11-01 <= pickup < 2025-12-01
q3 = """
SELECT COUNT(*) 
FROM green_taxi_data 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01' 
  AND trip_distance <= 1.0;
"""
try:
    print(run_query(q3))
except Exception as e:
    print(e)


print("\n--- Question 4 ---")
# Day with longest trip distance, trip_distance < 100
q4 = """
SELECT DATE(lpep_pickup_datetime) as pickup_day, MAX(trip_distance) as max_dist
FROM green_taxi_data
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_dist DESC
LIMIT 1;
"""
try:
    print(run_query(q4))
except Exception as e:
    print(e)


print("\n--- Question 5 ---")
# Pickup zone with largest total_amount on 2025-11-18
# We need to join with taxi_zone_lookup
q5 = """
SELECT "Zone", SUM(t.total_amount) as total
FROM green_taxi_data t
JOIN taxi_zone_lookup z ON t."PULocationID" = z."LocationID"
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY "Zone"
ORDER BY total DESC
LIMIT 1;
"""
try:
    print(run_query(q5))
except Exception as e:
    print(e)


print("\n--- Question 6 ---")
# Largest tip for passengers picked up in "East Harlem North" in Nov 2025
# Drop off zone with largest tip
q6 = """
SELECT z_drop."Zone", MAX(t.tip_amount) as max_tip
FROM green_taxi_data t
JOIN taxi_zone_lookup z_pick ON t."PULocationID" = z_pick."LocationID"
JOIN taxi_zone_lookup z_drop ON t."DOLocationID" = z_drop."LocationID"
WHERE z_pick."Zone" = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime < '2025-12-01'
GROUP BY z_drop."Zone"
ORDER BY max_tip DESC
LIMIT 1;
"""
try:
    print(run_query(q6))
except Exception as e:
    print(e)
