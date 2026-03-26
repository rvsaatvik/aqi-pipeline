# NYC Air Quality EL Pipeline

An end-to-end Extract–Load pipeline that ingests New York City air quality data from the JuHe AQI API twice daily, stores raw files in a Parquet landing zone, validates schema evolution, and loads into DuckDB for interactive exploration.

---

## Architecture

```
JuHe AQI API
     │
     ▼
┌──────────┐     ┌─────────────┐     ┌──────────────────────┐
│  extract  │────▶│  validate   │────▶│        load          │
│           │     │             │     │                      │
│ API call  │     │ Schema check│     │ DuckDB upsert        │
│ → Parquet │     │ (fail fast  │     │ daily_new_york_aqi   │
│ landing   │     │  on change) │     │                      │
└──────────┘     └─────────────┘     └──────────────────────┘
```

### Design decisions

**Decoupled landing zone** — the `extract` task always writes the raw API response to a timestamped Parquet file before any validation or loading. If the API spec changes and `validate` fails, the raw data is preserved on disk and can be replayed once the schema is updated.

**Explicit schema evolution detection** — `validate` checks that all required columns are present and numeric columns are still castable to `float64`. On mismatch it raises `SchemaEvolutionError` with a clear diff message. DuckDB is never written in this case.

**Idempotency at both layers** — the extractor checks whether a Parquet file for the current `execution_date` already exists and skips the API call if so. The loader performs a `DELETE … WHERE partition_date = ?` before every insert, so re-running the same DAG execution never duplicates rows.

**Rate-limit-safe retries** — the JuHe API enforces 10 requests/minute. Tenacity retries on `HTTPError` with a 6-second fixed wait (3 attempts max), staying safely within quota.

**Named Docker volume for DuckDB** — Windows NTFS bind mounts do not preserve Linux file ownership. DuckDB lives in a named Docker volume (`duckdb-data`) so `chown` works correctly and the database persists across container restarts.

---

## Project layout

```
.
├── dags/
│   └── carta_pipeline_dag.py   # Airflow DAG: extract >> validate >> load
├── pipeline/
│   ├── config.py               # Pydantic Settings (reads .env)
│   ├── extractor.py            # AQIExtractor — API fetch + Parquet write
│   ├── loader.py               # DuckDBLoader + validate_schema + SchemaEvolutionError
│   └── models.py               # AQIRecord (Pydantic v2, Decimal types, validators)
├── tests/
│   ├── test_api_key.py         # Live integration tests (pytest -m integration)
│   ├── test_extractor.py       # Unit tests — HTTP mock, idempotency, Parquet write
│   ├── test_loader.py          # Unit tests — schema validation, DuckDB upsert
│   └── test_models.py          # Unit tests — field validators (city, aqi, lat/lon)
├── data/                       # Created at runtime — landing Parquet files
├── docker-compose.yaml
├── .env.example
└── README.md
```

---

## Prerequisites

| Tool | Minimum version | Install |
|------|----------------|---------|
| Docker Desktop | 4.x | https://docs.docker.com/get-docker/ |
| Docker Compose | v2 (bundled with Docker Desktop) | — |
| Python | 3.11+ (for running tests locally) | https://python.org |
| pip | any | bundled with Python |

A JuHe API key is required. The steps below are all necessary — skipping any one of them results in `code: 10001 / invalid apikey` even if a key exists.

1. Register at **https://www.juhe.cn** (real-name / phone verification required)
2. Navigate to the AQI product page and click **立即申请 / Apply** to subscribe — the key is not active until you explicitly subscribe to this API
3. Go to **我的数据 (My Data)** and copy the key shown for the AQI subscription
4. Paste it as `API_KEY` in your `.env` file

> The API is hosted at `hub.juheapi.com` (not `apis.juhe.cn`). Make sure you are subscribed to the hub-hosted AQI product specifically.

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/rvsaatvik/aqi-pipeline.git
cd aqi-pipeline
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your values:

```dotenv
API_BASE_URL=https://hub.juheapi.com
API_KEY=<your_juhe_api_key>
API_SECRET=
DB_PATH=/opt/airflow/db/pipeline.duckdb
```

> `.env` is git-ignored and must never be committed — it contains your API key.

### 3. Start the stack

```bash
docker compose up -d
```

This starts:

| Service | URL | Purpose |
|---------|-----|---------|
| `airflow-webserver` | http://localhost:8080 | Airflow UI (admin / admin) |
| `airflow-scheduler` | — | Runs DAG tasks |
| `postgres` | — | Airflow metadata DB |
| `duckdb-ui` | http://localhost:4213 | DuckDB web UI |

First startup takes ~2 minutes while Airflow installs Python dependencies (`duckdb pandas pyarrow httpx tenacity pydantic`).

### 4. Verify Airflow is healthy

```bash
docker compose ps
```

All services should show `healthy` or `running`. Then open http://localhost:8080.

---

## Running the pipeline

### Trigger manually

1. Open http://localhost:8080
2. Find the `aqi_new_york_pipeline` DAG
3. Toggle it **on** (unpause)
4. Click **Trigger DAG ▶**

### Schedule

The DAG runs automatically at **00:00 UTC** and **12:00 UTC** once unpaused.

### Backfill historical dates

```bash
docker exec carta-airflow-scheduler-1 \
  airflow dags backfill aqi_new_york_pipeline \
  --start-date 2025-01-01 --end-date 2025-01-07
```

---

## Exploring data

Open **http://localhost:4213** (DuckDB UI). The pipeline database is available as the `pipeline` catalog:

```sql
-- All loaded records
SELECT * FROM pipeline.daily_new_york_aqi ORDER BY partition_date DESC;

-- AQI trend
SELECT partition_date, aqi, pm25, pm10
FROM pipeline.daily_new_york_aqi
ORDER BY partition_date;

-- Inspect raw API response
SELECT partition_date, record_metadata->>'$.response_ts' AS fetched_at
FROM pipeline.daily_new_york_aqi;
```

### DuckDB schema

```sql
CREATE TABLE daily_new_york_aqi (
    partition_date  DATE    NOT NULL,   -- date portion of execution_date
    city            VARCHAR,            -- "New York"
    aqi             DOUBLE,             -- Air Quality Index
    co              DOUBLE,             -- Carbon monoxide
    no2             DOUBLE,             -- Nitrogen dioxide
    o3              DOUBLE,             -- Ozone
    pm10            DOUBLE,             -- Particulate matter ≤10 µm
    pm25            DOUBLE,             -- Particulate matter ≤2.5 µm
    so2             DOUBLE,             -- Sulfur dioxide
    lat             DOUBLE,             -- Latitude
    lon             DOUBLE,             -- Longitude
    record_content  JSON    NOT NULL,   -- All AQI fields as JSON
    record_metadata JSON    NOT NULL    -- run_id, dag_id, triggered_by, raw_response
);
```

---

## Running tests

Install test dependencies locally:

```bash
pip install pytest pytest-mock requests-mock duckdb pandas pyarrow pydantic pydantic-settings python-dotenv tenacity requests
```

### Unit tests (no API key needed)

```bash
pytest tests/ -v --ignore=tests/test_api_key.py
```

### Live API integration tests

Requires a valid `API_KEY` in `.env`:

```bash
pytest tests/test_api_key.py -v -m integration
```

These tests make real HTTP calls to the JuHe API and verify:
- The key is accepted (response `code == "200"`)
- All required fields (`city`, `aqi`, `co`, `no2`, `o3`, `pm10`, `pm25`, `so2`, `geo`) are present
- `aqi > 0` and `geo.lat` / `geo.lon` are non-zero

---

## Stopping and cleanup

```bash
# Stop all containers (data persists in Docker volumes)
docker compose down

# Full teardown including all volumes (deletes DuckDB data)
docker compose down -v
```
