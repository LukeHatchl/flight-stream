"""Fabricates one bronze-layer Parquet snapshot for CI.

Real ADS-B data lives only on disk under the gitignored data/ directory —
CI has nothing to build dbt models against otherwise. The schema here must
stay in sync with collector/writer.py's SCHEMA.
"""
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        ("icao24", pa.string()),
        ("callsign", pa.string()),
        ("origin_country", pa.string()),
        ("time_position", pa.int64()),
        ("last_contact", pa.int64()),
        ("longitude", pa.float64()),
        ("latitude", pa.float64()),
        ("baro_altitude", pa.float64()),
        ("geo_altitude", pa.float64()),
        ("on_ground", pa.bool_()),
        ("velocity", pa.float64()),
        ("true_track", pa.float64()),
        ("vertical_rate", pa.float64()),
        ("squawk", pa.string()),
        ("position_source", pa.int64()),
        ("fetched_at", pa.timestamp("us", tz="UTC")),
    ]
)

fetched_at = datetime.now(timezone.utc)
row = {
    "icao24": "abc123",
    "callsign": "CI TEST",
    "origin_country": "United States",
    "time_position": int(fetched_at.timestamp()),
    "last_contact": int(fetched_at.timestamp()),
    "longitude": -76.6,
    "latitude": 39.0,
    "baro_altitude": 1000.0,
    "geo_altitude": 1050.0,
    "on_ground": False,
    "velocity": 100.0,
    "true_track": 90.0,
    "vertical_rate": 0.0,
    "squawk": "1200",
    "position_source": 0,
    "fetched_at": fetched_at,
}

partition_dir = Path("data") / "bronze" / f"date={fetched_at:%Y-%m-%d}" / f"hour={fetched_at:%H}"
partition_dir.mkdir(parents=True, exist_ok=True)

table = pa.Table.from_pylist([row], schema=SCHEMA)
out_path = partition_dir / f"snapshot_{int(fetched_at.timestamp())}.parquet"
pq.write_table(table, out_path)
print(f"Wrote CI fixture snapshot to {out_path}")
