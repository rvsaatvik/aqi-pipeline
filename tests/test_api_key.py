"""
Integration test — verifies the configured JuHe API key is valid and the
endpoint returns a well-formed AQI response for New York.

Run explicitly (requires a live API key in .env):
    pytest tests/test_api_key.py -v -m integration

Skipped automatically in the standard test suite (no API_KEY env var required).
"""
from __future__ import annotations

import os

import pytest
import requests


JUHE_URL = "https://hub.juheapi.com/aqi/v1/city"
CITY = "New York"


def _api_key() -> str | None:
    """Return the API key from the environment (loaded from .env by pytest via Settings)."""
    try:
        from pipeline.config import Settings
        return Settings().api_key
    except Exception:
        return os.getenv("API_KEY")


@pytest.mark.integration
def test_api_key_is_accepted():
    """API returns code '200' — key is valid and quota is not exhausted."""
    key = _api_key()
    if not key:
        pytest.skip("API_KEY not configured — skipping live API test")

    resp = requests.get(JUHE_URL, params={"city": CITY, "key": key}, timeout=15)
    resp.raise_for_status()
    payload = resp.json()

    assert str(payload.get("code")) == "200", (
        f"JuHe API rejected the key — code={payload.get('code')!r}, "
        f"msg={payload.get('msg')!r}"
    )


@pytest.mark.integration
def test_api_response_has_required_fields():
    """Successful response contains all expected AQI fields for New York."""
    key = _api_key()
    if not key:
        pytest.skip("API_KEY not configured — skipping live API test")

    resp = requests.get(JUHE_URL, params={"city": CITY, "key": key}, timeout=15)
    payload = resp.json()
    data = payload.get("data", {})

    required = {"city", "aqi", "co", "no2", "o3", "pm10", "pm25", "so2", "geo"}
    missing = required - set(data.keys())
    assert not missing, f"API response missing fields: {missing}"

    assert data["city"] == CITY, f"Expected city='New York', got {data['city']!r}"
    assert float(data["aqi"]) > 0, f"Expected aqi > 0, got {data['aqi']!r}"

    geo = data.get("geo", {})
    assert "lat" in geo and "lon" in geo, f"geo block missing lat/lon: {geo}"
    assert float(geo["lat"]) != 0 and float(geo["lon"]) != 0, "geo lat/lon are zero"
