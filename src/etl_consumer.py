"""
etl_consumer.py

Consumes from ride-events, batches records, validates each batch with
Great Expectations, then runs valid records through the surge engine
and writes results to SQLite.

Surge engine logic (rolling 5-min window, 10/20 request thresholds)
has been unit-tested separately: confirmed correct tier transitions
and window rollover behavior.

Usage:
    python src/etl_consumer.py
"""

import json
import sqlite3
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path

from dateutil import parser as date_parser
from kafka import KafkaConsumer

from data_validation import validate_batch

# --- Config ------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ride-events"
DB_PATH = str(Path(__file__).parent.parent / "rides_warehouse.db")

WINDOW_MINUTES = 5
MODERATE_THRESHOLD = 10
HEAVY_THRESHOLD = 20

BATCH_SIZE = 5          # validate every N records
BATCH_TIMEOUT_SEC = 10  # or after this many seconds, whichever comes first
# -------------------------------------------------------------------------

zone_event_times = defaultdict(deque)


def compute_surge(zone_id: int, event_time: datetime) -> float:
    history = zone_event_times[zone_id]
    history.append(event_time)

    cutoff = event_time - timedelta(minutes=WINDOW_MINUTES)
    while history and history[0] < cutoff:
        history.popleft()

    count = len(history)
    if count >= HEAVY_THRESHOLD:
        return 2.0
    elif count >= MODERATE_THRESHOLD:
        return 1.5
    else:
        return 1.0


def write_trip(conn, record: dict, surge_multiplier: float):
    final_fare = round(record["base_fare"] * surge_multiplier, 2)
    surge_flag = 1 if surge_multiplier > 1.0 else 0

    conn.execute(
        """
        INSERT OR REPLACE INTO fact_trips
            (trip_id, zone_id, timestamp, base_fare, surge_multiplier, final_fare, surge_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["trip_id"],
            record["zone_id"],
            record["timestamp"],
            record["base_fare"],
            surge_multiplier,
            final_fare,
            surge_flag,
        ),
    )
    conn.commit()


def process_batch(conn, batch: list, processed_count: list, dropped_count: list):
    valid, invalid, report = validate_batch(batch)
    dropped_count[0] += len(invalid)

    if invalid:
        print(f"  Validation dropped {len(invalid)} record(s). Report: {report}")

    for record in valid:
        try:
            ts = date_parser.parse(record["timestamp"])
        except (ValueError, TypeError, KeyError):
            dropped_count[0] += 1
            continue

        if record.get("spatial_coordinates") is None:
            dropped_count[0] += 1
            continue

        record["timestamp"] = ts.isoformat()
        surge_multiplier = compute_surge(record["zone_id"], ts)
        write_trip(conn, record, surge_multiplier)
        processed_count[0] += 1

    if processed_count[0] % 5 < len(valid) and valid:
        print(
            f"Processed {processed_count[0]} trips total (dropped {dropped_count[0]} total)"
        )


def build_consumer():
    max_retries = 15
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            print("Connected to Kafka.")
            return consumer
        except Exception as e:
            print(f"Kafka not ready yet (attempt {attempt}/{max_retries}): {e}")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after retries.")


def run():
    consumer = build_consumer()
    conn = sqlite3.connect(DB_PATH)

    print("Consumer started. Waiting for events...")
    processed_count = [0]
    dropped_count = [0]

    batch = []
    last_flush = time.time()

    for message in consumer:
        batch.append(message.value)

        should_flush = len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_TIMEOUT_SEC
        if should_flush and batch:
            process_batch(conn, batch, processed_count, dropped_count)
            batch = []
            last_flush = time.time()


if __name__ == "__main__":
    run()
