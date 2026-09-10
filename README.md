# FlightStream

Live ADS-B geospatial data pipeline — ingests aircraft positions from the OpenSky Network, lands raw snapshots in partitioned Parquet, transforms through a bronze/silver/gold dbt architecture, orchestrates with Airflow, and serves a live map dashboard.

## Architecture

```
OpenSky API ──► Collector (60s poll) ──► Bronze Parquet (local / S3)
                                                │
                                    Airflow DAG (10 min)
                                    load → dbt run → dbt test
                                                │
                              ┌─────────────────┼──────────────┐
                              ▼                 ▼              ▼
                        DuckDB + spatial    dbt models     Streamlit
                                           silver / gold   live map
```

**Bounding box:** Baltimore/DC metro (BWI, DCA, IAD)
`lamin=38.5  lomin=-77.7  lamax=39.7  lomax=-76.2`

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python + opensky-api (OAuth2) |
| Raw storage | Parquet, partitioned by date/hour |
| Orchestration | Apache Airflow |
| Transformation | dbt (dbt-duckdb) |
| Warehouse | DuckDB + spatial extension |
| Infrastructure | Terraform (S3 bucket, IAM) |
| Dashboard | Streamlit + pydeck |

> **Note:** the official `opensky-api` Python client isn't published on PyPI — it's installed straight from GitHub (see `collector/requirements.txt`). No action needed, just don't be surprised seeing a `git+https://...` line in requirements instead of a version pin.

## Setup

### Prerequisites

- Docker + Docker Compose
- OpenSky Network account with an API client (`credentials.json`)

### 1. Add credentials

Download your `credentials.json` from the OpenSky Account page and place it in the project root. It is gitignored.

### 2. Start services

```bash
docker compose up --build
```

- Airflow UI: http://localhost:8080 (admin / admin)
- Collector starts polling immediately; snapshots land in `data/bronze/`

To run just the collector (e.g. while iterating on it without the Airflow stack):

```bash
docker compose up --build collector
```

### 3. Verify the collector is working

Tail the collector's logs — it logs one line per poll with the snapshot path and row count:

```bash
docker compose logs -f collector
```

```
2026-09-10 15:42:03 INFO __main__: Wrote 98 state(s) to /opt/flightstream/data/bronze/date=2026-09-10/hour=15/snapshot_1789054923.parquet
2026-09-10 15:43:01 INFO __main__: Wrote 95 state(s) to /opt/flightstream/data/bronze/date=2026-09-10/hour=15/snapshot_1789054981.parquet
```

Snapshots accumulate on the host under `data/bronze/date=YYYY-MM-DD/hour=HH/`:

```bash
find data/bronze -type f
```

To inspect the actual contents of a snapshot (schema + a few rows), run a one-off Python command inside the container:

```bash
docker compose exec collector python -c "
import pyarrow.parquet as pq
t = pq.read_table('/opt/flightstream/data/bronze/date=2026-09-10/hour=15/snapshot_1789054923.parquet')
print(t.schema)
print(t.num_rows, 'rows')
print(t.slice(0, 3).to_pydict())
"
```

(Swap in the actual `date=`/`hour=`/`snapshot_*.parquet` path from your own `find data/bronze -type f` output.)

Retry/backoff on 429/5xx (via `tenacity`) only triggers on real transient OpenSky failures, so there's nothing to manually exercise — if you want to confirm it engages, watch the logs during a spell of OpenSky rate-limiting/outage: a retried poll logs nothing extra (`tenacity` retries silently), but a poll that exhausts all 5 attempts logs `Poll failed; will retry next interval` and picks back up on the next 60s cycle rather than crashing the container.

### 4. Configuration

Collector behavior is controlled by environment variables set in `docker-compose.yml`:

| Variable | Default | Purpose |
|---|---|---|
| `FLIGHTSTREAM_BBOX` | `38.5,-77.7,39.7,-76.2` | `lamin,lomin,lamax,lomax` — bounding box to poll |
| `FLIGHTSTREAM_POLL_INTERVAL_SECONDS` | `60` | Seconds between polls |
| `FLIGHTSTREAM_CREDENTIALS_PATH` | `/opt/flightstream/credentials.json` | Path to the OpenSky OAuth2 credentials file |
| `FLIGHTSTREAM_DATA_DIR` | `/opt/flightstream/data` | Base directory bronze snapshots are written under |

### 5. Run dbt locally (optional)

```bash
cd dbt
pip install dbt-duckdb
dbt build --profiles-dir .
```

### 6. Stop / clean up

```bash
docker compose stop        # pause containers, keep them for a fast restart
docker compose down        # remove containers + network, keeps the postgres volume (Airflow metadata) and data/bronze/
```

Avoid `docker compose down -v` unless you actually want to wipe the Airflow metadata database.

## Data model

- **Bronze** — raw snapshots, one Parquet file per poll, partitioned `date=YYYY-MM-DD/hour=HH/`
- **Silver** — `stg_states` (typed, de-duped), `dim_airports`, `dim_aircraft`
- **Gold** — `fct_active_flights`, `fct_flight_tracks`, `mart_airport_proximity`, `mart_traffic_heatmap`, `mart_flights_by_country`

## Build phases

- [x] Phase 0 — Scaffold
- [x] Phase 1 — Extractor MVP
- [ ] Phase 2 — DuckDB + first dbt models
- [ ] Phase 3 — Airflow orchestration
- [ ] Phase 4 — Geospatial marts
- [ ] Phase 5 — Terraform
- [ ] Phase 6 — Dashboard
- [ ] Phase 7 — Production polish
