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

### 5. Run dbt

dbt runs locally against the same `data/` directory the collector writes to — no separate service needed. Let the collector accumulate at least one snapshot first (see step 3), then:

```bash
cd dbt
pip install dbt-duckdb
FLIGHTSTREAM_DATA_DIR="$(cd ../data && pwd)" dbt build --profiles-dir .
```

`FLIGHTSTREAM_DATA_DIR` must be an absolute path here — dbt models read bronze Parquet directly off disk via DuckDB's `read_parquet()`, and a relative path resolves against dbt's internal run directory, not your shell's cwd.

This builds, in order:
- **`stg_states`** (view) — reads every file under `data/bronze/**/*.parquet`, types and cleans the columns, drops on-ground/null-position rows, and de-dupes.
- **`airports`** (seed) — 1,772 US airports with an ICAO code, sourced from OurAirports and filtered down from their full ~86k-row global list (see `dbt/seeds/airports.csv`).
- **`fct_active_flights`** (table) — every aircraft from the single most recent poll, i.e. what's airborne in the bbox right now.

The first `dbt build` on a fresh machine takes ~60s one-time to download DuckDB's `spatial` extension (declared in `dbt/profiles.yml`); every run after that finishes in under a second for this data volume.

To poke at the results directly instead of trusting the `dbt build` output:

```bash
cd dbt
FLIGHTSTREAM_DATA_DIR="$(cd ../data && pwd)" python3 -c "
import duckdb
con = duckdb.connect('../data/warehouse.duckdb')
print(con.execute('select count(*) from main.stg_states').fetchall())
print(con.execute('select icao24, callsign, latitude, longitude, velocity_knots, heading, snapshot_ts from main.fct_active_flights limit 5').fetchall())
"
```

Or open it in the DuckDB CLI: `duckdb data/warehouse.duckdb` then `select * from fct_active_flights limit 10;`.

### 6. Airflow orchestration

Once the collector has some bronze data, Airflow can run the same dbt work automatically instead of you invoking it by hand. The DAG (`airflow/dags/flightstream_dag.py`, id `flightstream_pipeline`) runs every 10 minutes:

1. **`check_new_bronze_data`** — fails fast with a clear message if `data/bronze/` has no Parquet files at all (rather than letting dbt fail on it with a cryptic DuckDB error).
2. **`dbt_run_staging`** — `dbt run --select staging` (rebuilds `stg_states`).
3. **`dbt_test`** — `dbt test`. Currently a no-op (0 tests defined yet — that's Phase 7), but it's already wired in as a gate: once real tests exist, a failure here blocks the next step.
4. **`dbt_run_marts`** — `dbt run --select marts` (refreshes `fct_active_flights`), only runs if the test gate passes.

The Airflow image is custom-built (`airflow/Dockerfile`) on top of `apache/airflow:2.9.3` with `dbt-duckdb` installed (see `airflow/requirements.txt`) — the stock image doesn't ship dbt. If you change that Dockerfile or requirements file, rebuild before starting:

```bash
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d
```

DAGs start **paused** (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`), so the schedule won't fire until you unpause it — either toggle it on in the UI (http://localhost:8080) or:

```bash
docker compose exec airflow-webserver airflow dags unpause flightstream_pipeline
```

To test it immediately instead of waiting for the next 10-minute mark:

```bash
docker compose exec airflow-webserver airflow dags trigger flightstream_pipeline
docker compose exec airflow-webserver airflow dags list-runs -d flightstream_pipeline
```

The Airflow UI's Grid view for `flightstream_pipeline` is the easiest way to watch task-by-task status and read logs per task. Note the very first `dbt_run_staging` after a fresh `docker compose up --build` can take several minutes — each container has its own DuckDB extension cache, so the one-time `spatial` extension download (see step 5) happens again per container, not just per machine. Every run after that is fast.

### 7. Stop / clean up

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
- [x] Phase 2 — DuckDB + first dbt models
- [x] Phase 3 — Airflow orchestration
- [ ] Phase 4 — Geospatial marts
- [ ] Phase 5 — Terraform
- [ ] Phase 6 — Dashboard
- [ ] Phase 7 — Production polish
