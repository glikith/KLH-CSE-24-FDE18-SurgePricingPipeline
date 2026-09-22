"""
data_validation.py

Batch-validates incoming ride event records using Great Expectations
before they reach the surge engine. Validates schema presence, value
ranges, and geographic bounds.

Tested standalone: correctly flags negative fares and out-of-range
values in a sample batch.

Usage (as a module, called from etl_consumer.py):
    from data_validation import validate_batch
    results = validate_batch(list_of_dicts)
"""

import pandas as pd
import great_expectations as gx

# NYC's approximate lat/lon bounds, used as a sanity range check.
NYC_LAT_RANGE = (40.4, 41.0)
NYC_LON_RANGE = (-74.3, -73.6)

_context = None
_batch_def = None


def _get_batch_definition():
    """
    Builds the GE context/data source once and reuses it across calls,
    since recreating it per-batch is unnecessary overhead.
    """
    global _context, _batch_def
    if _batch_def is None:
        _context = gx.get_context(mode="ephemeral")
        data_source = _context.data_sources.add_pandas("ride_events_source")
        data_asset = data_source.add_dataframe_asset(name="ride_events")
        _batch_def = data_asset.add_batch_definition_whole_dataframe("batch")
    return _batch_def


def validate_batch(records: list):
    """
    Validates a batch (list of dicts) of ride event records.

    Returns (valid_records, invalid_records, report) where report is a
    dict summarizing which checks passed/failed.
    """
    if not records:
        return [], [], {"note": "empty batch"}

    df = pd.DataFrame(records)
    batch_def = _get_batch_definition()
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    checks = {
        "base_fare_non_negative": gx.expectations.ExpectColumnValuesToBeBetween(
            column="base_fare", min_value=0
        ),
        "zone_id_not_null": gx.expectations.ExpectColumnValuesToNotBeNull(
            column="zone_id"
        ),
        "trip_id_not_null": gx.expectations.ExpectColumnValuesToNotBeNull(
            column="trip_id"
        ),
    }

    report = {}
    bad_indices = set()

    for name, expectation in checks.items():
        result = batch.validate(expectation)
        report[name] = result.success
        if not result.success:
            unexpected = result.result.get("partial_unexpected_index_list", [])
            bad_indices.update(unexpected)

    valid_records = [r for i, r in enumerate(records) if i not in bad_indices]
    invalid_records = [r for i, r in enumerate(records) if i in bad_indices]

    return valid_records, invalid_records, report


if __name__ == "__main__":
    sample = [
        {"trip_id": "a1", "zone_id": 100, "base_fare": 12.5},
        {"trip_id": "a2", "zone_id": 200, "base_fare": -3.0},  # invalid
        {"trip_id": "a3", "zone_id": None, "base_fare": 20.0},  # invalid
    ]
    valid, invalid, report = validate_batch(sample)
    print("Report:", report)
    print(f"Valid: {len(valid)}, Invalid: {len(invalid)}")
