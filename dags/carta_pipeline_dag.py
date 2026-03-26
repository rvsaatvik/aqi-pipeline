"""
AQI New York EL Pipeline DAG
  extract  >>  validate  >>  load

extract  : Calls the JuHe AQI API → writes raw response to a timestamped
           Parquet file in the landing zone. Always succeeds (no schema
           enforcement), so raw data is preserved even if downstream fails.

validate : Reads the landing Parquet → checks that all required columns are
           present and numeric columns are castable to float. Raises
           SchemaEvolutionError on any mismatch so spec changes are caught
           before anything reaches DuckDB.

load     : Inserts the validated record into DuckDB daily_new_york_aqi with
           partition_date, individual AQI columns, record_content JSON, and
           record_metadata JSON. Idempotent: delete-then-insert on partition_date.

Schedule : Twice daily at 00:00 and 12:00 UTC.
Catchup  : Disabled by default — enable and use `airflow dags backfill` for
           historical runs.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=6),  # 6 s respects the 10 req/min rate limit
}


# ── task callables ────────────────────────────────────────────────────────────

def extract_task(**context):
    """Fetch AQI data and write raw Parquet to landing zone."""
    from pipeline.config import Settings
    from pipeline.extractor import AQIExtractor

    settings = Settings()
    execution_dt: datetime = context["execution_date"]

    logger.info("=== EXTRACT starting | execution_date=%s ===", execution_dt)
    logger.info("Landing dir: %s", settings.landing_dir)
    logger.info("API base URL: %s", settings.api_base_url)

    extractor = AQIExtractor(api_key=settings.api_key, landing_dir=settings.landing_dir)
    raw, parquet_path = extractor.extract(execution_dt)

    data = raw.get("data", {})
    logger.info(
        "API response received | city=%s aqi=%s pm25=%s pm10=%s o3=%s",
        data.get("city"), data.get("aqi"), data.get("pm25"),
        data.get("pm10"), data.get("o3"),
    )
    logger.info("Landing Parquet: %s (size=%s bytes)", parquet_path,
                parquet_path.stat().st_size if parquet_path.exists() else "n/a")

    context["ti"].xcom_push(key="parquet_path", value=str(parquet_path))
    context["ti"].xcom_push(key="raw_response", value=raw)
    logger.info("=== EXTRACT complete | parquet_path=%s ===", parquet_path)


def validate_task(**context):
    """Read landing Parquet and validate schema. Fail explicitly on spec changes."""
    import pandas as pd
    from pipeline.loader import validate_schema

    parquet_path: str = context["ti"].xcom_pull(task_ids="extract", key="parquet_path")
    logger.info("=== VALIDATE starting | parquet_path=%s ===", parquet_path)

    df = pd.read_parquet(parquet_path)
    logger.info("Parquet loaded | shape=%s columns=%s", df.shape, list(df.columns))
    logger.info("Raw dtypes:\n%s", df.dtypes.to_string())

    # Raises SchemaEvolutionError if any column type or name has changed.
    validate_schema(df)
    logger.info("Schema validation passed — all required columns present and numeric-castable")

    # Drop raw_response (large, belongs in record_metadata) and convert all
    # pandas/numpy types to plain Python strings so XCom JSON serialization works.
    row = df.drop(columns=["raw_response"], errors="ignore").iloc[0]
    record_content = {
        k: (None if pd.isna(v) else str(v))
        for k, v in row.items()
    }
    logger.info("record_content prepared: %s", record_content)

    context["ti"].xcom_push(key="record_content", value=record_content)
    logger.info("=== VALIDATE complete ===")


def load_task(**context):
    """Insert validated AQI record into DuckDB daily_new_york_aqi."""
    from pipeline.config import Settings
    from pipeline.loader import DuckDBLoader

    settings = Settings()
    execution_dt: datetime = context["execution_date"]
    dag_run = context["dag_run"]
    partition_date = execution_dt.date()

    logger.info("=== LOAD starting | partition_date=%s run_id=%s ===",
                partition_date, dag_run.run_id)

    record_content: dict = context["ti"].xcom_pull(task_ids="validate", key="record_content")
    raw_response: dict = context["ti"].xcom_pull(task_ids="extract", key="raw_response")

    logger.info("record_content to load: %s", record_content)

    record_metadata = {
        "response_ts": datetime.now(tz=timezone.utc).isoformat(),
        "dag_id": context["dag"].dag_id,
        "run_id": dag_run.run_id,
        "triggered_by": dag_run.run_type,
        "raw_response": raw_response,
    }
    logger.info("record_metadata (excluding raw_response): dag_id=%s run_id=%s triggered_by=%s response_ts=%s",
                record_metadata["dag_id"], record_metadata["run_id"],
                record_metadata["triggered_by"], record_metadata["response_ts"])

    logger.info("Connecting to DuckDB at %s", settings.db_path)
    with DuckDBLoader(db_path=settings.db_path, data_dir="/opt/airflow/data") as loader:
        count = loader.load_aqi(
            record_content=record_content,
            record_metadata=record_metadata,
            partition_date=partition_date,
        )
        logger.info("DuckDB write complete | rows for partition_date=%s: %s", partition_date, count)

    logger.info("=== LOAD complete | partition_date=%s ===", partition_date)


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="aqi_new_york_pipeline",
    default_args=default_args,
    description="Extract NYC AQI from JuHe API → validate schema → load into DuckDB",
    schedule="0 0,12 * * *",   # twice daily: midnight and noon UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,              # set to True and use `airflow dags backfill` for historical runs
    tags=["aqi", "el"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=extract_task)
    validate = PythonOperator(task_id="validate", python_callable=validate_task)
    load = PythonOperator(task_id="load", python_callable=load_task)

    extract >> validate >> load
