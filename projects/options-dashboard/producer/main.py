import os
import json
import time
import random
import datetime
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# Kafka configuration
kafka_config = {
    "bootstrap.servers": os.getenv("KAFKA_BROKER", "localhost:9092"),
    "client.id": "mock-options-producer",
}
producer = Producer(kafka_config)
TOPIC_NAME = "options-trades-stream"


def main():
    print("Starting simulated options-trade producer...")
    symbols = [
        "SPY241220C0500",
        "AAPL240119C0150",
        "MSFT240119P0300",
        "TSLA240119C0250",
        "NVDA240119C0400",
    ]

    try:
        while True:
            data = {
                "event_type": "T",
                "sym": random.choice(symbols),
                "price": round(random.uniform(1.50, 45.00), 2),
                "size": random.randint(1, 100),
                "timestamp": int(datetime.datetime.utcnow().timestamp() * 1000),
                "conditions": [212],
            }
            print(f"Sending: {data}")
            producer.produce(TOPIC_NAME, value=json.dumps(data).encode("utf-8"))
            producer.flush()
            time.sleep(random.uniform(0.1, 0.5))
    except KeyboardInterrupt:
        print("Stopping client.")


if __name__ == "__main__":
    main()
