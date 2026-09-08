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

### 3. Run dbt locally (optional)

```bash
cd dbt
pip install dbt-duckdb
dbt build --profiles-dir .
```

## Data model

- **Bronze** — raw snapshots, one Parquet file per poll, partitioned `date=YYYY-MM-DD/hour=HH/`
- **Silver** — `stg_states` (typed, de-duped), `dim_airports`, `dim_aircraft`
- **Gold** — `fct_active_flights`, `fct_flight_tracks`, `mart_airport_proximity`, `mart_traffic_heatmap`, `mart_flights_by_country`

## Build phases

- [x] Phase 0 — Scaffold
- [ ] Phase 1 — Extractor MVP
- [ ] Phase 2 — DuckDB + first dbt models
- [ ] Phase 3 — Airflow orchestration
- [ ] Phase 4 — Geospatial marts
- [ ] Phase 5 — Terraform
- [ ] Phase 6 — Dashboard
- [ ] Phase 7 — Production polish
