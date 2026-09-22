"""
populate_zones.py

One-time setup script. Reads all 263 zones from the real TLC shapefile
and inserts them into dim_zones.

Usage:
    python src/populate_zones.py
"""

import sqlite3
from pathlib import Path
from zone_lookup import get_all_zones

DB_PATH = str(Path(__file__).parent.parent / "rides_warehouse.db")


def populate():
    zones = get_all_zones()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.executemany(
        "INSERT OR REPLACE INTO dim_zones (zone_id, zone_name, city_region) VALUES (?, ?, ?)",
        zones,
    )
    conn.commit()

    count = cur.execute("SELECT COUNT(*) FROM dim_zones").fetchone()[0]
    print(f"dim_zones populated: {count} zones inserted.")
    conn.close()


if __name__ == "__main__":
    populate()
