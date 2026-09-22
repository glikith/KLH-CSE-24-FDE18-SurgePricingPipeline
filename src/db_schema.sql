CREATE TABLE IF NOT EXISTS dim_zones (
    zone_id     INTEGER PRIMARY KEY,
    zone_name   VARCHAR(100) NOT NULL,
    city_region VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id          VARCHAR(64) PRIMARY KEY,
    zone_id          INTEGER NOT NULL,
    timestamp        DATETIME NOT NULL,
    base_fare        FLOAT NOT NULL,
    surge_multiplier FLOAT NOT NULL,
    final_fare       FLOAT NOT NULL,
    surge_flag       INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (zone_id) REFERENCES dim_zones(zone_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_trips_zone_id ON fact_trips(zone_id);
CREATE INDEX IF NOT EXISTS idx_fact_trips_timestamp ON fact_trips(timestamp);
