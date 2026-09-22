"""
zone_lookup.py

Resolves lat/lon coordinates to official NYC TLC taxi zones using
point-in-polygon lookup against the real TLC Taxi Zones shapefile.

Tested against: JFK Airport, Times Square, Central Park -- all resolve
to the correct zone name and borough.
"""

import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path

SHAPEFILE_PATH = str(Path(__file__).parent.parent / "data" / "taxi_zones" / "taxi_zones.shp")

_zones_gdf = None


def _load_zones():
    global _zones_gdf
    if _zones_gdf is None:
        gdf = gpd.read_file(SHAPEFILE_PATH)
        # Shapefile ships in EPSG:2263 (feet-based NY State Plane).
        # Convert to WGS84 (EPSG:4326) to match plain lat/lon coordinates.
        gdf = gdf.to_crs(epsg=4326)
        _zones_gdf = gdf
    return _zones_gdf


def get_bounds():
    """Returns (min_lon, min_lat, max_lon, max_lat) covering all zones."""
    gdf = _load_zones()
    return tuple(gdf.total_bounds)


def latlon_to_zone(lat: float, lon: float):
    """
    Returns (zone_id, zone_name, borough) for a lat/lon, or
    (None, None, None) if the point falls outside all zones.
    """
    gdf = _load_zones()
    point = Point(lon, lat)

    match = gdf[gdf.contains(point)]
    if match.empty:
        return None, None, None

    row = match.iloc[0]
    return int(row["LocationID"]), str(row["zone"]), str(row["borough"])


def get_all_zones():
    """Returns [(zone_id, zone_name, borough), ...] for all 263 zones."""
    gdf = _load_zones()
    return [
        (int(row["LocationID"]), str(row["zone"]), str(row["borough"]))
        for _, row in gdf.iterrows()
    ]


if __name__ == "__main__":
    zid, zname, borough = latlon_to_zone(40.6413, -73.7781)
    print(f"JFK test -> zone_id={zid}, zone_name={zname}, borough={borough}")
    print(f"NYC bounds: {get_bounds()}")
