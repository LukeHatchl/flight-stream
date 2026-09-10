"""Polls the configured StateSource every FLIGHTSTREAM_POLL_INTERVAL_SECONDS
and writes each snapshot to the bronze Parquet layer. Runs forever."""
from __future__ import annotations

import logging
import os
import time

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from extractor import OpenSkySource, RetryableSourceError, StateSource
from writer import ParquetSnapshotWriter

logger = logging.getLogger(__name__)

# Baltimore/DC metro (BWI, DCA, IAD) — spec section 4 default bbox.
DEFAULT_BBOX = (38.5, -77.7, 39.7, -76.2)


def _bbox_from_env() -> tuple[float, float, float, float]:
    raw = os.environ.get("FLIGHTSTREAM_BBOX")
    if not raw:
        return DEFAULT_BBOX
    lamin, lomin, lamax, lomax = (float(v) for v in raw.split(","))
    return (lamin, lomin, lamax, lomax)


@retry(
    retry=retry_if_exception_type(RetryableSourceError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _fetch_with_retry(source: StateSource) -> list[dict]:
    return source.fetch_states()


def run(source: StateSource, writer: ParquetSnapshotWriter, poll_interval: float) -> None:
    while True:
        poll_start = time.monotonic()
        try:
            states = _fetch_with_retry(source)
            path = writer.write(states)
            logger.info("Wrote %d state(s) to %s", len(states), path)
        except Exception:
            logger.exception("Poll failed; will retry next interval")

        elapsed = time.monotonic() - poll_start
        time.sleep(max(0.0, poll_interval - elapsed))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    data_dir = os.environ.get("FLIGHTSTREAM_DATA_DIR", "./data")
    credentials_path = os.environ.get("FLIGHTSTREAM_CREDENTIALS_PATH")
    poll_interval = float(os.environ.get("FLIGHTSTREAM_POLL_INTERVAL_SECONDS", "60"))

    bbox = _bbox_from_env()
    source = OpenSkySource(bbox=bbox, credentials_path=credentials_path)
    writer = ParquetSnapshotWriter(base_dir=data_dir)

    logger.info("Starting collector: bbox=%s interval=%ss data_dir=%s", bbox, poll_interval, data_dir)
    try:
        run(source, writer, poll_interval)
    finally:
        source.close()


if __name__ == "__main__":
    main()
