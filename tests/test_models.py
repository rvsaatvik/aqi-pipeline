"""Data quality tests for AQIRecord."""
from decimal import Decimal

import pytest
from pydantic import ValidationError

from pipeline.models import AQIRecord

_VALID = dict(city="New York", aqi=Decimal("53"), lat=Decimal("40.7128"), lon=Decimal("-74.0060"))


def test_city_never_null():
    with pytest.raises(ValidationError):
        AQIRecord(**{**_VALID, "city": None})


def test_aqi_must_be_positive():
    with pytest.raises(ValidationError):
        AQIRecord(**{**_VALID, "aqi": Decimal("0")})
    with pytest.raises(ValidationError):
        AQIRecord(**{**_VALID, "aqi": Decimal("-5")})


def test_lat_lon_must_not_be_null():
    with pytest.raises(ValidationError):
        AQIRecord(city="New York", aqi=Decimal("53"))  # lat and lon missing
