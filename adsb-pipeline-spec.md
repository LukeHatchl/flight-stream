# FlightStream — Live ADS-B Geospatial Data Pipeline

**Project spec / build brief.** Hand this to Claude Code section by section. Build in the phase order given: each phase leaves you with something that runs, so you're never stuck with a half-working system.

---

## 1. What this is

A production-style data platform that continuously ingests live aircraft positions from the OpenSky Network, lands raw snapshots in object storage, transforms and models them with dbt, runs geospatial analytics in a warehouse, orchestrates the whole batch cycle with Airflow, provisions cloud infrastructure with Terraform, and serves a live map + analytics dashboard.
---

## 2. Architecture at a glance

Two cooperating pieces, a near-real-time collector plus a scheduled transform pipeline:

```
                          ┌─────────────────────────────────────┐
                          │           COLLECTOR (service)        │
   OpenSky API  ───────►  │  polls every 60s, writes raw Parquet │
   /states/all (bbox)     │  snapshots partitioned by date/hour  │
                          └───────────────┬─────────────────────┘
                                          │  (BRONZE: raw snapshots)
                                          ▼
                          ┌─────────────────────────────────────┐
                          │        AIRFLOW DAG (every 10 min)    │
                          │  1. load new bronze files            │
                          │  2. run dbt (staging → marts)        │
                          │  3. run dbt tests + data-quality     │
                          │  4. refresh geospatial marts         │
                          └───────────────┬─────────────────────┘
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    ▼                     ▼                     ▼
              DuckDB warehouse       dbt models            Streamlit
          (+ spatial extension)   SILVER / GOLD           live map + charts
```

- **Collector** runs continuously (a small Python service) so ingestion feels live. Keeping it separate from Airflow is deliberate and defensible: Airflow orchestrates *batch* work well, but is the wrong tool for sub-minute polling.
- **Airflow** orchestrates the transform cycle — load, dbt build, tests, mart refresh — every 10 minutes. This is where Airflow genuinely earns its place in the story.
- **Medallion layers:** bronze (raw), silver (cleaned/typed), gold (analytics marts). 
---

## 3. Tech stack (and why each is here)

| Layer | Tool | Why / resume payoff |
|---|---|---|
| Ingestion | Python + `requests` / `opensky-api` | Live source handling, auth, retries, rate-limit pacing |
| Raw storage | Parquet on local FS → S3 | Columnar storage, partitioning; S3 = your existing AWS strength |
| Orchestration | **Apache Airflow** | You already know it — showcase DAGs orchestrating dbt + tests |
| Transformation | **dbt** (dbt-duckdb) | The gap you're closing; staging → marts, tests, docs |
| Warehouse | **DuckDB** + `spatial` ext | Zero-cost, fast, and the spatial extension does the geo joins |
| Geo reference | OurAirports `airports.csv` | Dimension table for airport-proximity / arrivals analytics |
| Infrastructure | **Terraform** | The other gap you're closing; provisions S3 (+ optional BigQuery) |
| Serving | Streamlit + pydeck | Live map, fastest path to a demoable UI |
| Quality | dbt tests (+ optional Great Expectations) | Data-quality story, which is your BCBS/EpochGeo strength |
| CI | GitHub Actions | Runs dbt build + tests on every push; the "production" polish |

**Cost:** everything local (DuckDB, Airflow in Docker, Streamlit, dbt) is $0. The Terraform-provisioned cloud pieces sit inside AWS free tier / BigQuery free tier — pennies, or nothing if you tear them down after demoing.

---

## 4. The data source (OpenSky Network) — important 2026 details

- **Endpoint:** `GET https://opensky-network.org/api/states/all` with a bounding box: `?lamin=&lomin=&lamax=&lomax=` (min/max latitude and longitude). Returns current *state vectors* for all aircraft in the box.
- **Auth changed in March 2026.** Basic username/password auth is gone. You now register a free account, create an **API client** on your Account page, download `credentials.json`, and the Python library authenticates via **OAuth2 client credentials** — tokens refresh automatically:
  ```python
  from opensky_api import OpenSkyApi, TokenManager
  api = OpenSkyApi(token_manager=TokenManager.from_json_file("credentials.json"))
  states = api.get_states(bbox=(lamin, lomin, lamax, lomax))
  ```
- **Rate limits:** anonymous ≈ 100 calls/day (not enough for continuous polling — so an account is effectively required). Authenticated ≈ 4,000 calls/day at 5-second resolution. Polling one bounding box **every 60s = 1,440 calls/day**, comfortably inside budget with headroom to widen later.
- **Fallback sources** if OpenSky is flaky or you want a no-auth option: `adsb.lol` and `airplanes.live` both expose free community ADS-B feeds. Keep the extractor behind an interface so the source is swappable.

**State vector fields you'll use:** `icao24` (unique aircraft id), `callsign`, `origin_country`, `time_position`, `last_contact`, `longitude`, `latitude`, `baro_altitude`, `geo_altitude`, `on_ground`, `velocity`, `true_track` (heading), `vertical_rate`, `squawk`, `position_source`.

**Suggested bounding box — your backyard.** Baltimore/DC metro captures BWI, DCA, and IAD (three major airports = rich arrivals/departures signal), and it's a nice personal touch since it's local to you:
```
lamin=38.5  lomin=-77.7  lamax=39.7  lomax=-76.2
```
Widen to the whole Mid-Atlantic or CONUS once it works — just watch the call budget.

---

## 5. Data model

**Bronze — raw snapshots.** One Parquet file per poll, partitioned `bronze/date=YYYY-MM-DD/hour=HH/snapshot_<epoch>.parquet`. Store fields as-received plus a `fetched_at` timestamp. Never mutate bronze.

**Silver — cleaned state vectors (`stg_states`).** Typed, de-duplicated, junk filtered:
- cast lat/lon/altitude/velocity to proper numerics; trim `callsign` whitespace
- drop rows with null position or `on_ground = true` (unless you want ground movement)
- convert `true_track` to a usable heading; convert velocity m/s → knots if you like
- add `snapshot_ts` from `fetched_at`

**Silver — dimensions.** `dim_airports` from OurAirports (icao code, name, lat/lon, elevation, type), `dim_aircraft` (distinct `icao24` first-seen/last-seen — a simple SCD-style track of aircraft observed).

**Gold — analytics marts.** These are what make it a *project* and not a data dump. Each answers a real question:
- `fct_active_flights` — current snapshot: what's airborne in the box right now, with speed/altitude/heading.
- `fct_flight_tracks` — join consecutive snapshots by `icao24` to reconstruct trajectories over time (the "connect the dots into a flight path" mart — great for the map).
- `mart_airport_proximity` — **geospatial join**: aircraft within N km of each airport, low-altitude + descending → inferred arrivals; climbing → inferred departures. Uses DuckDB's `spatial` extension (`ST_Distance`, `ST_Point`).
- `mart_traffic_heatmap` — position density grid over the box (busiest corridors).
- `mart_flights_by_country` — origin-country breakdown over time.

The geospatial marts are your differentiator — most portfolio pipelines stop at "group by and count." Point-in-polygon and distance joins are exactly the ArcGIS/OSM instincts you already have, now expressed in the modern stack.

---

## 6. Repository structure

```
flightstream/
├── README.md                  # architecture diagram, setup, screenshots, "what this shows"
├── docker-compose.yml         # Airflow + collector + (optional MinIO for local S3)
├── .github/workflows/ci.yml   # dbt build + tests on push
├── collector/
│   ├── extractor.py           # OpenSky client w/ source interface + retries
│   ├── writer.py              # Parquet partition writer (local or S3)
│   └── run_collector.py       # the 60s polling loop / service entrypoint
├── infra/                     # Terraform
│   ├── main.tf                # S3 bronze bucket, IAM, lifecycle policy
│   ├── bigquery.tf            # optional: warehouse dataset in the cloud
│   ├── variables.tf
│   └── outputs.tf
├── airflow/
│   └── dags/flightstream_dag.py   # load → dbt run → dbt test → refresh marts
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml           # dbt-duckdb target
│   ├── models/staging/        # stg_states.sql, stg_airports.sql
│   ├── models/marts/          # fct_*.sql, mart_*.sql
│   ├── seeds/airports.csv     # OurAirports reference
│   └── tests/                 # schema tests + custom data-quality tests
├── dashboard/
│   └── app.py                 # Streamlit live map + charts
└── data/                      # local warehouse.duckdb + bronze/ (gitignored)
```

---

## 7. Build phases

Do them in order. Each ends with something runnable.

**Phase 0 — Scaffold.** Repo, `.gitignore`, `docker-compose.yml`, README skeleton, OpenSky account + `credentials.json` (kept out of git). *Done when:* `docker compose up` starts cleanly.

**Phase 1 — Extractor MVP (this is your "it works" moment).** `extractor.py` pulls one snapshot from the bbox; `writer.py` writes it to a partitioned Parquet file; `run_collector.py` loops every 60s with retry + backoff on 429/5xx. Put the source behind a small interface so OpenSky/adsb.lol are swappable. *Done when:* Parquet snapshots are accumulating on disk.

**Phase 2 — DuckDB + first dbt models.** Wire dbt-duckdb; build `stg_states` reading the bronze Parquet; seed `airports.csv`; build `fct_active_flights`. *Done when:* `dbt build` produces queryable marts.

**Phase 3 — Airflow orchestration.** DAG on a 10-minute schedule: load new bronze → `dbt run` → `dbt test` → refresh marts. *Done when:* the DAG runs green end-to-end on schedule.

**Phase 4 — Geospatial marts (your differentiator).** Enable DuckDB `spatial`; build `fct_flight_tracks`, `mart_airport_proximity`, `mart_traffic_heatmap`. *Done when:* you can query "planes within 15km of BWI, descending" and get sane results.

**Phase 5 — Terraform.** Provision the S3 bronze bucket with IAM + a lifecycle policy (expire raw after N days — mirrors your real EpochGeo work), point the writer at S3. Optional: a BigQuery dataset as a cloud warehouse target to show multi-warehouse fluency. *Done when:* `terraform apply` stands it up and `terraform destroy` tears it down cleanly.

**Phase 6 — Dashboard.** Streamlit + pydeck: live map of current aircraft (heading-rotated markers), reconstructed tracks as lines, a couple of charts (traffic over time, flights by country, arrivals/departures at each airport). *Done when:* it refreshes and shows moving planes.

**Phase 7 — Production polish.** dbt schema tests (not_null, unique on `icao24`+`snapshot_ts`, accepted ranges on lat/lon/altitude) + custom quality tests (no impossible speeds, positions inside the bbox); GitHub Actions running `dbt build`; a README with the architecture diagram, setup steps, and screenshots/GIF of the live map. *Done when:* a stranger could clone it, follow the README, and run it.

MVP = Phases 0–3 (a real orchestrated pipeline). Phases 4–7 are what make it interview-grade. Ship the MVP first so the dated "Independent Projects" resume line is true early, then layer up.

---

## 8. What this demonstrates (map for your resume + interviews)

Keep this section — it's your talking-point cheat sheet.

- **dbt:** "Modeled raw ADS-B state vectors through a staging → marts layered architecture with schema and custom data-quality tests." *(closes the dbt gap)*
- **Terraform:** "Provisioned the S3 raw-storage bucket, IAM policies, and lifecycle rules as code." *(closes the IaC gap; mirrors real EpochGeo work)*
- **Warehouse:** "Built geospatial analytics in DuckDB using its spatial extension for distance and point-in-polygon joins." *(closes the warehouse gap + shows your geo edge)*
- **Orchestration:** "Authored an Airflow DAG orchestrating incremental loads, dbt transformations, and data-quality checks on a schedule." *(reinforces existing Airflow strength)*
- **Streaming-ish ingestion:** "Designed a near-real-time collector decoupled from batch orchestration, with retry/backoff and swappable sources." *(architectural judgment — a senior signal)*
- **Data quality:** "Enforced quality gates catching impossible velocities and out-of-bounds coordinates before they reached marts." *(your BCBS/EpochGeo QA thread)*

One résumé bullet, once it's built:
> **FlightStream — Live geospatial data platform (Oct 2025–Present).** Built an end-to-end pipeline ingesting live ADS-B aircraft positions (~1,440 snapshots/day) into partitioned S3 storage, modeled with dbt across a bronze/silver/gold architecture, orchestrated with Airflow, provisioned with Terraform, and served via a Streamlit live map with geospatial arrivals/departures analytics (DuckDB spatial).

---

## 9. Stretch goals (only after Phase 7)

- **Anomaly detection:** flag aircraft squawking emergency codes (7500/7600/7700) or with implausible tracks.
- **Airspace sectors:** load sector polygons and do true point-in-polygon occupancy counts.
- **Historical backfill + trends:** accumulate weeks of data, chart daily traffic patterns per airport.
- **Swap DuckDB → BigQuery/Snowflake** as the gold target to prove warehouse-agnostic modeling.
- **dbt exposures + docs site** published via GitHub Pages.

---

## 10. First message to Claude Code

> I'm building FlightStream, a live ADS-B geospatial data pipeline (spec attached). Start with Phase 0 and Phase 1: scaffold the repo structure from section 6, then build the collector — an OpenSky extractor (OAuth2 client-credentials auth via credentials.json, bounding box for the Baltimore/DC metro), a Parquet partition writer, and a 60-second polling loop with retry/backoff on 429 and 5xx. Put the data source behind an interface so I can swap in adsb.lol later. Don't build later phases yet.

Feed it one phase at a time — you'll get cleaner, more reviewable output, and reviewing each phase yourself is exactly the "directing AI agents and reviewing generated output" skill the 2026 market is filtering for.
