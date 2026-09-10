"""Bronze-layer Parquet writer — one partitioned file per poll. Never mutates
existing files; every poll (even an empty one) leaves its own snapshot."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from extractor import STATE_FIELDS

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


class ParquetSnapshotWriter:
    """Writes bronze snapshots under <base_dir>/bronze/date=YYYY-MM-DD/hour=HH/."""

    def __init__(self, base_dir: str):
        self._base_dir = Path(base_dir)

    def write(self, states: list[dict], fetched_at: datetime | None = None) -> Path:
        fetched_at = fetched_at or datetime.now(timezone.utc)

        partition_dir = self._base_dir / "bronze" / f"date={fetched_at:%Y-%m-%d}" / f"hour={fetched_at:%H}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        rows = [{**{field: state.get(field) for field in STATE_FIELDS}, "fetched_at": fetched_at} for state in states]
        table = pa.Table.from_pylist(rows, schema=SCHEMA)

        out_path = partition_dir / f"snapshot_{int(fetched_at.timestamp())}.parquet"
        pq.write_table(table, out_path)
        return out_path
