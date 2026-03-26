"""
Pydantic models for the JuHe AQI API response.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, field_validator


class AQIRecord(BaseModel):
    city: str
    aqi: Decimal
    co: Optional[Decimal] = None
    no2: Optional[Decimal] = None
    o3: Optional[Decimal] = None
    pm10: Optional[Decimal] = None
    pm25: Optional[Decimal] = None
    so2: Optional[Decimal] = None
    lat: Decimal
    lon: Decimal

    @field_validator("aqi")
    @classmethod
    def aqi_must_be_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("aqi must be > 0")
        return v

    @classmethod
    def from_api_response(cls, data: dict) -> "AQIRecord":
        """Parse the `data` object from a JuHe AQI API response."""
        geo = data.get("geo", {})
        return cls(
            city=data["city"],
            aqi=data["aqi"],
            co=data.get("co"),
            no2=data.get("no2"),
            o3=data.get("o3"),
            pm10=data.get("pm10"),
            pm25=data.get("pm25"),
            so2=data.get("so2"),
            lat=geo["lat"],
            lon=geo["lon"],
        )

    def to_dict(self) -> dict:
        return {k: str(v) if isinstance(v, Decimal) else v for k, v in self.model_dump().items()}
