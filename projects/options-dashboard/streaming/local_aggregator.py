import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

TOPIC_NAME = "polygon-options-stream"


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise RuntimeError(
        f"Missing required environment variable: {name}. "
        "Populate projects/options-dashboard/.env from .env.example."
    )


def floor_to_minute(timestamp_ms: int) -> datetime:
    event_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return event_time.replace(second=0, microsecond=0, tzinfo=None)


def get_db_connection():
    conn = psycopg2.connect(
        dbname=get_required_env("POSTGRES_DATABASE"),
        user=get_required_env("POSTGRES_USER"),
        password=get_required_env("POSTGRES_PASSWORD"),
        host=get_required_env("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    conn.autocommit = True
    return conn


def flush_aggregates(cursor, aggregates, keys_to_flush):
    for key in keys_to_flush:
        stats = aggregates.get(key)
        if not stats or stats["total_volume"] == 0:
            continue

        window_start, symbol = key
        window_end = window_start + timedelta(minutes=1)
        vwap = stats["notional"] / stats["total_volume"]

        cursor.execute(
            """
            INSERT INTO options_data (window_start, window_end, sym, vwap, total_volume)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (window_start, sym)
            DO UPDATE SET
                window_end = EXCLUDED.window_end,
                vwap = EXCLUDED.vwap,
                total_volume = EXCLUDED.total_volume
            """,
            (window_start, window_end, symbol, vwap, stats["total_volume"]),
        )


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BROKER", "localhost:9092"),
            "group.id": "options_local_aggregator",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC_NAME])

    conn = get_db_connection()
    cur = conn.cursor()
    aggregates = defaultdict(lambda: {"notional": 0.0, "total_volume": 0})
    last_flush = time.time()

    print("Starting local Kafka-to-PostgreSQL aggregator...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is not None:
                if msg.error():
                    continue

                payload = json.loads(msg.value().decode("utf-8"))
                symbol = payload["sym"]
                price = float(payload["price"])
                size = int(payload["size"])
                window_start = floor_to_minute(int(payload["timestamp"]))

                key = (window_start, symbol)
                aggregates[key]["notional"] += price * size
                aggregates[key]["total_volume"] += size

            now = time.time()
            if now - last_flush >= 2:
                flush_aggregates(cur, aggregates, list(aggregates.keys()))

                current_minute = datetime.now(timezone.utc).replace(
                    second=0, microsecond=0, tzinfo=None
                )
                stale_keys = [key for key in aggregates if key[0] < current_minute]
                for key in stale_keys:
                    del aggregates[key]

                last_flush = now
    except KeyboardInterrupt:
        print("Stopping local aggregator.")
    finally:
        flush_aggregates(cur, aggregates, list(aggregates.keys()))
        consumer.close()
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
