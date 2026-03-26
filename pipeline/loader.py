"""
DuckDB loader for daily_new_york_aqi.

Landing Parquet files are the source of truth; this module reads them,
validates that the API spec has not changed, and upserts into DuckDB.

Schema evolution is detected explicitly: if a column's values can no longer
be cast to the expected numeric type, SchemaEvolutionError is raised BEFORE
any write to DuckDB, ensuring the raw Parquet is always preserved.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Columns that must be castable to numeric (float64) in the landing Parquet.
EXPECTED_NUMERIC_COLS: frozenset[str] = frozenset(
    {"aqi", "co", "no2", "o3", "pm10", "pm25", "so2", "lat", "lon"}
)
# Columns that must always be present in the landing Parquet.
REQUIRED_COLS: frozenset[str] = frozenset(
    {"city", "aqi", "co", "no2", "o3", "pm10", "pm25", "so2", "lat", "lon"}
)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS daily_new_york_aqi (
    partition_date  DATE    NOT NULL,
    city            VARCHAR,
    aqi             DOUBLE,
    co              DOUBLE,
    no2             DOUBLE,
    o3              DOUBLE,
    pm10            DOUBLE,
    pm25            DOUBLE,
    so2             DOUBLE,
    lat             DOUBLE,
    lon             DOUBLE,
    record_content  JSON    NOT NULL,
    record_metadata JSON    NOT NULL
)
"""

# Migration: add individual columns to tables created before this schema change.
_MIGRATE_COLUMNS = [
    "ALTER TABLE daily_new_york_aqi ADD COLUMN IF NOT EXISTS city  VARCHAR",
    "ALTER TABLE daily_new_york_aqi ADD COLUMN IF NOT EXISTS aqi   DOUBLE",
    "ALTER TABLE daily_new_york_aqi ADD COLUMN IF NOT EXISTS co    DOUBLE",
    "ALTER TABLE daily_new_york_aqi ADD COLUMN IF NOT EXISTS no2   DOUBLE",
    "ALTER TABLE daily_new_york_aqi ADD COLUMN IF NOT EXISTS o3    DOUBLE",
    "ALTER TABLE daily_new_york_aqi ADD COLUMN IF NOT EXISTS pm10  DOUBLE",
    "ALTER TABLE daily_new_york_aqi ADD COLUMN IF NOT EXISTS pm25  DOUBLE",
    "ALTER TABLE daily_new_york_aqi ADD COLUMN IF NOT EXISTS so2   DOUBLE",
    "ALTER TABLE daily_new_york_aqi ADD COLUMN IF NOT EXISTS lat   DOUBLE",
    "ALTER TABLE daily_new_york_aqi ADD COLUMN IF NOT EXISTS lon   DOUBLE",
]


class SchemaEvolutionError(Exception):
    """Raised when the landing Parquet schema deviates from the expected API spec."""


def validate_schema(df: pd.DataFrame) -> None:
    """Validate landing Parquet columns against the expected AQI schema.

    Raises SchemaEvolutionError when:
    - Any required column is absent (renamed / dropped field in API)
    - Any numeric column contains values that cannot be cast to float64

    The raw Parquet is never written by this function — it only reads the
    DataFrame, so a failure here leaves the landing file intact.
    """
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise SchemaEvolutionError(
            f"API response is missing expected columns: {sorted(missing)}"
        )

    for col in EXPECTED_NUMERIC_COLS:
        if col not in df.columns:
            continue
        non_null = df[col].dropna()
        non_null = non_null[non_null != ""]  # API returns "" for missing pollutants
        if non_null.empty:
            continue
        try:
            pd.to_numeric(non_null, errors="raise")
        except (ValueError, TypeError) as exc:
            raise SchemaEvolutionError(
                f"Column '{col}' cannot be cast to numeric — possible API spec change. "
                f"Sample value: {non_null.iloc[0]!r}"
            ) from exc


class DuckDBLoader:
    def __init__(self, db_path: str, data_dir: str = "data"):
        self.db_path = db_path
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(db_path)
        self.con.execute(_CREATE_TABLE)
        for stmt in _MIGRATE_COLUMNS:
            self.con.execute(stmt)

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> "DuckDBLoader":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def load_aqi(
        self,
        record_content: dict[str, Any],
        record_metadata: dict[str, Any],
        partition_date: date,
    ) -> int:
        """Upsert a single AQI record into daily_new_york_aqi.

        Idempotent: deletes any existing row for partition_date before inserting,
        so re-running the same DAG execution never duplicates rows.
        """
        self.con.execute(
            "DELETE FROM daily_new_york_aqi WHERE partition_date = ?",
            [partition_date],
        )
        def _f(v: Any) -> float | None:
            return float(v) if v not in (None, "") else None

        self.con.execute(
            """
            INSERT INTO daily_new_york_aqi (
                partition_date,
                city, aqi, co, no2, o3, pm10, pm25, so2, lat, lon,
                record_content, record_metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                partition_date,
                record_content.get("city"),
                _f(record_content.get("aqi")),
                _f(record_content.get("co")),
                _f(record_content.get("no2")),
                _f(record_content.get("o3")),
                _f(record_content.get("pm10")),
                _f(record_content.get("pm25")),
                _f(record_content.get("so2")),
                _f(record_content.get("lat")),
                _f(record_content.get("lon")),
                json.dumps(record_content),
                json.dumps(record_metadata),
            ],
        )
        count = self.con.execute(
            "SELECT COUNT(*) FROM daily_new_york_aqi WHERE partition_date = ?",
            [partition_date],
        ).fetchone()[0]
        logger.info("Loaded AQI record for partition_date=%s into daily_new_york_aqi", partition_date)
        return count

    def query(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()
