"""Tests for AQIExtractor."""
from datetime import datetime

import pytest

from pipeline.extractor import AQIExtractor

MOCK_RESPONSE = {
    "code": "200",
    "msg": "Success",
    "data": {
        "city": "New York",
        "aqi": "53",
        "co": "350.5",
        "no2": "15.2",
        "o3": "58.6",
        "pm10": "22.1",
        "pm25": "14.7",
        "so2": "1.9",
        "geo": {"lat": "40.7128", "lon": "-74.0060"},
    },
}

EXECUTION_DT = datetime(2025, 3, 25, 12, 0, 0)


@pytest.fixture
def extractor(tmp_path):
    return AQIExtractor(api_key="test-key", landing_dir=str(tmp_path / "landing"))


def test_extract_uses_apikey_query_param(extractor, requests_mock):
    requests_mock.get("https://hub.juheapi.com/aqi/v1/city", json=MOCK_RESPONSE)
    extractor.extract(EXECUTION_DT)
    assert requests_mock.last_request.qs["apikey"] == ["test-key"]


def test_extract_uses_q_for_city(extractor, requests_mock):
    requests_mock.get("https://hub.juheapi.com/aqi/v1/city", json=MOCK_RESPONSE)
    extractor.extract(EXECUTION_DT)
    assert requests_mock.last_request.qs["q"] == ["New York"]


def test_extract_no_bearer_header(extractor, requests_mock):
    requests_mock.get("https://hub.juheapi.com/aqi/v1/city", json=MOCK_RESPONSE)
    extractor.extract(EXECUTION_DT)
    assert "Authorization" not in requests_mock.last_request.headers


def test_extract_returns_raw_response(extractor, requests_mock):
    requests_mock.get("https://hub.juheapi.com/aqi/v1/city", json=MOCK_RESPONSE)
    raw, _ = extractor.extract(EXECUTION_DT)
    assert raw["data"]["city"] == "New York"
    assert raw["data"]["aqi"] == "53"


def test_extract_writes_parquet(extractor, requests_mock):
    requests_mock.get("https://hub.juheapi.com/aqi/v1/city", json=MOCK_RESPONSE)
    _, path = extractor.extract(EXECUTION_DT)
    assert path.exists()
    assert path.suffix == ".parquet"


def test_extract_idempotent_skips_api_on_existing_file(extractor, requests_mock):
    """Second call with the same execution_dt must not hit the API again."""
    requests_mock.get("https://hub.juheapi.com/aqi/v1/city", json=MOCK_RESPONSE)
    extractor.extract(EXECUTION_DT)
    call_count = requests_mock.call_count

    raw, _ = extractor.extract(EXECUTION_DT)
    assert requests_mock.call_count == call_count  # no additional HTTP call
    assert raw["data"]["city"] == "New York"       # still returns correct data
