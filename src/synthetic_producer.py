"""
synthetic_producer.py

Generates synthetic ride requests with coordinates constrained to real
NYC zone boundaries (from the TLC shapefile), and publishes them to
Kafka as JSON. This mimics a live ride-hailing event stream without
depending on an external API.

Points are generated within the real bounding box of all zones, then
resolved to an actual zone via point-in-polygon lookup. Points that
land outside every zone (gaps/water areas within the bounding box) are
retried up to MAX_RETRIES times, then skipped -- this is expected
real-world behavior, not a bug.

Usage:
    python src/synthetic_producer.py
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from zone_lookup import latlon_to_zone, get_bounds

# --- Config ------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ride-events"

EVENTS_PER_SECOND = 4.0   # throttle: 1 event every 2 seconds on average
BASE_FARE_MIN = 5.0
BASE_FARE_MAX = 45.0
MAX_RETRIES = 10          # retries per event to find a point inside a real zone
# -------------------------------------------------------------------------


def random_point_in_bounds(bounds):
    min_lon, min_lat, max_lon, max_lat = bounds
    lat = random.uniform(min_lat, max_lat)
    lon = random.uniform(min_lon, max_lon)
    return lat, lon


def generate_event(bounds):
    """
    Generates one valid ride event. 
    Simulates demand surges by concentrating 40% of requests around JFK Airport.
    """
    # 40% chance to target a busy hotspot (JFK Airport approximate coordinates)
    if random.random() < 0.40:
        lat = random.uniform(40.640, 40.655)
        lon = random.uniform(-73.795, -73.770)
        zone_id, zone_name, _ = latlon_to_zone(lat, lon)
        if zone_id is not None:
            return {
                "trip_id": str(uuid.uuid4()),
                "zone_id": zone_id,
                "spatial_coordinates": {"lat": lat, "lon": lon},
                "base_fare": round(random.uniform(BASE_FARE_MIN, BASE_FARE_MAX), 2),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    # Standard random generation for other zones
    for _ in range(MAX_RETRIES):
        lat, lon = random_point_in_bounds(bounds)
        zone_id, zone_name, _ = latlon_to_zone(lat, lon)
        if zone_id is not None:
            return {
                "trip_id": str(uuid.uuid4()),
                "zone_id": zone_id,
                "spatial_coordinates": {"lat": lat, "lon": lon},
                "base_fare": round(random.uniform(BASE_FARE_MIN, BASE_FARE_MAX), 2),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    return None


def build_producer():
    max_retries = 15
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Connected to Kafka.")
            return producer
        except Exception as e:
            print(f"Kafka not ready yet (attempt {attempt}/{max_retries}): {e}")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after retries.")


def run():
    bounds = get_bounds()
    print(f"Zone bounds loaded: {bounds}")

    producer = build_producer()
    sleep_interval = 1.0 / EVENTS_PER_SECOND

    sent = 0
    skipped = 0

    print("Producer started. Generating synthetic ride events...")
    try:
        while True:
            event = generate_event(bounds)
            if event is None:
                skipped += 1
            else:
                producer.send(KAFKA_TOPIC, value=event)
                producer.flush()
                sent += 1
                if sent % 10 == 0:
                    print(f"Sent {sent} events (skipped {skipped} invalid points)")

            time.sleep(sleep_interval)
    except KeyboardInterrupt:
        print(f"\nStopped. Total sent: {sent}, skipped: {skipped}")


if __name__ == "__main__":
    run()
