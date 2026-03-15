import json
from kafka import KafkaConsumer

def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=json_deserializer,
    consumer_timeout_ms=5000 # stop after 5 seconds of inactivity
)

trip_distance_count = 0

print("Consuming messages...")
for message in consumer:
    trip_data = message.value
    if 'trip_distance' in trip_data and trip_data['trip_distance'] > 5.0:
        trip_distance_count += 1

print(f"Number of trips with trip_distance > 5.0: {trip_distance_count}")
