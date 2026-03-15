import json
import pandas as pd
from kafka import KafkaProducer
from time import time

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Initialize the producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

# Load data and keep required columns
columns_to_keep = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]

print("Loading pandas data...")
df = pd.read_parquet('green_tripdata_2025-10.parquet', columns=columns_to_keep)

# Helper function to process each row
def process_row(row):
    # Convert dates to strings
    row_dict = row.to_dict()
    row_dict['lpep_pickup_datetime'] = str(row_dict['lpep_pickup_datetime'])
    row_dict['lpep_dropoff_datetime'] = str(row_dict['lpep_dropoff_datetime'])
    return row_dict

print("Sending data...")
t0 = time()

for _, row in df.iterrows():
    row_dict = process_row(row)
    producer.send('green-trips', value=row_dict)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
