"""Tests for DuckDB loader and schema validation."""
import json
from datetime import date

import pandas as pd
import pytest

from pipeline.loader import DuckDBLoader, SchemaEvolutionError, validate_schema

_PARTITION = date(2025, 3, 25)

_RECORD_CONTENT = {
    "city": "New York",
    "aqi": "53",
    "co": "350.5",
    "no2": "15.2",
    "o3": "58.6",
    "pm10": "22.1",
    "pm25": "14.7",
    "so2": "1.9",
    "lat": "40.7128",
    "lon": "-74.0060",
}

_RECORD_METADATA = {
    "response_ts": "2025-03-25T12:00:00+00:00",
    "dag_id": "aqi_new_york_pipeline",
    "run_id": "scheduled__2025-03-25T12:00:00",
    "triggered_by": "scheduled",
    "raw_response": {},
}


@pytest.fixture
def loader(tmp_path):
    with DuckDBLoader(db_path=str(tmp_path / "test.duckdb"), data_dir=str(tmp_path)) as l:
        yield l


def test_load_aqi_creates_row(loader):
    count = loader.load_aqi(_RECORD_CONTENT, _RECORD_METADATA, _PARTITION)
    assert count == 1


def test_load_aqi_partition_date_stored(loader):
    loader.load_aqi(_RECORD_CONTENT, _RECORD_METADATA, _PARTITION)
    df = loader.query("SELECT partition_date FROM daily_new_york_aqi")
    assert str(df["partition_date"][0]) == "2025-03-25"


def test_load_aqi_record_content_json(loader):
    loader.load_aqi(_RECORD_CONTENT, _RECORD_METADATA, _PARTITION)
    df = loader.query("SELECT record_content FROM daily_new_york_aqi")
    content = json.loads(df["record_content"][0])
    assert content["city"] == "New York"
    assert content["aqi"] == "53"


def test_load_aqi_record_metadata_json(loader):
    loader.load_aqi(_RECORD_CONTENT, _RECORD_METADATA, _PARTITION)
    df = loader.query("SELECT record_metadata FROM daily_new_york_aqi")
    meta = json.loads(df["record_metadata"][0])
    assert meta["dag_id"] == "aqi_new_york_pipeline"


def test_load_aqi_idempotent(loader):
    """Inserting twice for the same partition_date must not duplicate rows."""
    loader.load_aqi(_RECORD_CONTENT, _RECORD_METADATA, _PARTITION)
    loader.load_aqi(_RECORD_CONTENT, _RECORD_METADATA, _PARTITION)
    df = loader.query("SELECT COUNT(*) AS n FROM daily_new_york_aqi")
    assert df["n"][0] == 1


def _valid_df():
    return pd.DataFrame([{
        "city": "New York", "aqi": "53", "co": "350.5", "no2": "15.2",
        "o3": "58.6", "pm10": "22.1", "pm25": "14.7", "so2": "1.9",
        "lat": "40.7128", "lon": "-74.0060",
    }])


def test_validate_schema_passes_on_valid_data():
    validate_schema(_valid_df())  # must not raise


def test_validate_schema_raises_on_non_numeric_aqi():
    df = _valid_df().copy()
    df["aqi"] = "N/A"
    with pytest.raises(SchemaEvolutionError, match="aqi"):
        validate_schema(df)


def test_validate_schema_raises_on_missing_column():
    df = _valid_df().drop(columns=["lat"])
    with pytest.raises(SchemaEvolutionError, match="lat"):
        validate_schema(df)
