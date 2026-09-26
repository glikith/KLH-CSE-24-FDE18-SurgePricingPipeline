"""
synthetic_producer.py

Generates synthetic ride requests with coordinates constrained to real
NYC zone boundaries (from the TLC shapefile), and publishes them to
Kafka as JSON. This mimics a live ride-hailing event stream without
depending on an external API.

Simulates dynamic, rotating demand surges across 10 major NYC hubs,
switching randomly every 30 seconds so multiple zones experience
surge pricing, peak transitions, and cooldowns.

Usage:
    python src/synthetic_producer.py
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from zone_lookup import get_bounds, latlon_to_zone

# --- Config ------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ride-events"

EVENTS_PER_SECOND = 4.0   # Generates enough throughput to hit 10 & 20 rolling thresholds
BASE_FARE_MIN = 5.0
BASE_FARE_MAX = 45.0
MAX_RETRIES = 10          # Retries per event to find a point inside a real zone

# 10 Major NYC Demand Hotspots
HOTSPOTS = [
    {"name": "JFK Airport", "lat": (40.640, 40.655), "lon": (-73.795, -73.770)},
    {"name": "LaGuardia Airport", "lat": (40.770, 40.780), "lon": (-73.880, -73.865)},
    {"name": "Midtown Manhattan / Times Square", "lat": (40.750, 40.760), "lon": (-73.990, -73.980)},
    {"name": "Financial District", "lat": (40.705, 40.715), "lon": (-74.015, -74.005)},
    {"name": "Williamsburg", "lat": (40.710, 40.722), "lon": (-73.965, -73.950)},
    {"name": "Downtown Brooklyn / DUMBO", "lat": (40.695, 40.705), "lon": (-73.995, -73.985)},
    {"name": "Long Island City", "lat": (40.742, 40.753), "lon": (-73.952, -73.938)},
    {"name": "Upper East Side", "lat": (40.770, 40.782), "lon": (-73.960, -73.948)},
    {"name": "Greenwich Village / SoHo", "lat": (40.725, 40.735), "lon": (-74.005, -73.993)},
    {"name": "Astoria", "lat": (40.762, 40.775), "lon": (-73.930, -73.915)},
]

HOTSPOT_SWITCH_INTERVAL_SEC = 30  # Switch target hotspot randomly every 30 seconds
current_hotspot = random.choice(HOTSPOTS)
last_hotspot_switch = time.time()
# -------------------------------------------------------------------------


def update_hotspot_if_needed():
    """Randomly selects a new active hotspot every HOTSPOT_SWITCH_INTERVAL_SEC."""
    global current_hotspot, last_hotspot_switch
    now = time.time()
    if now - last_hotspot_switch >= HOTSPOT_SWITCH_INTERVAL_SEC:
        current_hotspot = random.choice(HOTSPOTS)
        last_hotspot_switch = now
        print(f"\n[SURGE SHIFT] Demand spike moved randomly to: {current_hotspot['name']}")


def random_point_in_bounds(bounds):
    min_lon, min_lat, max_lon, max_lat = bounds
    lat = random.uniform(min_lat, max_lat)
    lon = random.uniform(min_lon, max_lon)
    return lat, lon


def generate_event(bounds):
    """
    Generates one ride event.
    50% of events target the actively surging random hotspot.
    50% of events are distributed across general background NYC traffic.
    """
    update_hotspot_if_needed()

    # 50% chance to target the active hotspot zone
    if random.random() < 0.50:
        lat = random.uniform(*current_hotspot["lat"])
        lon = random.uniform(*current_hotspot["lon"])
        zone_id, zone_name, _ = latlon_to_zone(lat, lon)
        if zone_id is not None:
            return {
                "trip_id": str(uuid.uuid4()),
                "zone_id": zone_id,
                "spatial_coordinates": {"lat": lat, "lon": lon},
                "base_fare": round(random.uniform(BASE_FARE_MIN, BASE_FARE_MAX), 2),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    # Background random traffic across the rest of the city
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
    print(f"Initial surge active at: {current_hotspot['name']}")

    producer = build_producer()
    sleep_interval = 1.0 / EVENTS_PER_SECOND

    sent = 0
    skipped = 0

    print("Producer started. Generating dynamic ride events...")
    try:
        while True:
            event = generate_event(bounds)
            if event is None:
                skipped += 1
            else:
                producer.send(KAFKA_TOPIC, value=event)
                producer.flush()
                sent += 1
                if sent % 25 == 0:
                    print(f"Sent {sent} events (skipped {skipped} invalid points)")

            time.sleep(sleep_interval)
    except KeyboardInterrupt:
        print(f"\nStopped. Total sent: {sent}, skipped: {skipped}")


if __name__ == "__main__":
    run()