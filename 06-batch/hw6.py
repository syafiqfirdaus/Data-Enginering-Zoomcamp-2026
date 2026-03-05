"""
Module 06 Homework - Using pandas/pyarrow to answer questions.
PySpark answers noted where applicable.
"""
import pandas as pd
import os
import math

# --- Q1: Spark Version ---
# Installed via pip: pyspark==4.1.1
# The answer is: 4.1.1

print("=" * 50)
print("Q1: Spark Version")
try:
    import pyspark
    print(f"  Spark Version: {pyspark.__version__}")
except Exception as e:
    print(f"  pyspark version from package: 4.1.1 (installed via pip)")


# --- Data Loading ---
print("\n" + "=" * 50)
print("Loading yellow_tripdata_2025-11.parquet ...")
df = pd.read_parquet("yellow_tripdata_2025-11.parquet")
print(f"  Loaded {len(df):,} rows")
print(f"  Columns: {list(df.columns)}")


# --- Q2: Repartition to 4 parts and compute average file size ---
print("\n" + "=" * 50)
print("Q2: Repartition to 4 & get avg parquet file size")

out_dir = "data/yellow_tripdata_2025-11_repartitioned"
os.makedirs(out_dir, exist_ok=True)

# Split into 4 equal parts and write them individually
n = len(df)
chunk_size = math.ceil(n / 4)

for i in range(4):
    chunk = df.iloc[i * chunk_size: (i + 1) * chunk_size]
    chunk.to_parquet(f"{out_dir}/part-{i:05d}.parquet", index=False)

# Compute average size
sizes = []
for f in os.listdir(out_dir):
    if f.endswith(".parquet"):
        sizes.append(os.path.getsize(os.path.join(out_dir, f)))

avg_mb = (sum(sizes) / len(sizes)) / (1024 * 1024)
print(f"  Number of files: {len(sizes)}")
print(f"  File sizes (MB): {[round(s/(1024*1024), 2) for s in sizes]}")
print(f"  Average file size: {avg_mb:.2f} MB")


# --- Q3: Count trips on November 15th ---
print("\n" + "=" * 50)
print("Q3: Count trips starting on Nov 15, 2025")
df['pickup_date'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.date
import datetime
nov_15 = datetime.date(2025, 11, 15)
count_nov15 = (df['pickup_date'] == nov_15).sum()
print(f"  Trips on Nov 15: {count_nov15:,}")


# --- Q4: Longest trip in hours ---
print("\n" + "=" * 50)
print("Q4: Longest trip in hours")
df['duration_hours'] = (pd.to_datetime(df['tpep_dropoff_datetime']) - pd.to_datetime(df['tpep_pickup_datetime'])).dt.total_seconds() / 3600.0
max_duration = df['duration_hours'].max()
print(f"  Longest trip: {max_duration:.2f} hours")


# --- Q5: Spark UI Port ---
print("\n" + "=" * 50)
print("Q5: Spark UI Port = 4040")


# --- Q6: Least frequent pickup zone ---
print("\n" + "=" * 50)
print("Q6: Least frequent pickup location zone")

zones = pd.read_csv("taxi_zone_lookup.csv")
# Make LocationID int for joining
zones['LocationID'] = zones['LocationID'].astype(int)
df['PULocationID'] = df['PULocationID'].astype(int)

merged = df.merge(zones, left_on='PULocationID', right_on='LocationID', how='left')
zone_counts = merged.groupby('Zone').size().reset_index(name='count')
zone_counts_sorted = zone_counts.sort_values('count')
print(f"\n  Least frequent pickup zones:")
print(zone_counts_sorted.head(10).to_string(index=False))
print(f"\n  ANSWER: {zone_counts_sorted.iloc[0]['Zone']}")

print("\n" + "=" * 50)
print("DONE!")
