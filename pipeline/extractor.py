"""
AQI extractor — fetches air quality data for New York from the JuHe API.
Writes the raw API response to a Parquet landing file named by execution datetime.
Idempotent: if the landing file already exists, skips the API call and loads from disk.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

CITY = "New York"


class AQIExtractor:
    BASE_URL = "https://hub.juheapi.com"
    ENDPOINT = "/aqi/v1/city"

    def __init__(self, api_key: str, landing_dir: str):
        self.api_key = api_key
        self.landing_dir = Path(landing_dir)
        self.landing_dir.mkdir(parents=True, exist_ok=True)

    def extract(self, execution_dt: datetime) -> tuple[dict[str, Any], Path]:
        """Fetch AQI data and persist to landing Parquet.

        Returns (raw_response_dict, parquet_path).
        Idempotent: a file for this execution_dt already on disk skips the API call.
        """
        parquet_path = self._landing_path(execution_dt)

        if parquet_path.exists():
            logger.info("Landing file already exists, skipping API call: %s", parquet_path)
            raw = self._load_raw_from_parquet(parquet_path)
            return raw, parquet_path

        raw = self._fetch()
        self._write_parquet(raw, parquet_path)
        return raw, parquet_path

    # ── private ──────────────────────────────────────────────────────────────

    def _landing_path(self, execution_dt: datetime) -> Path:
        ms = execution_dt.microsecond // 1000
        filename = execution_dt.strftime("%Y%m%d_%H%M%S") + f"{ms:03d}.parquet"
        return self.landing_dir / filename

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(6),  # 6 s between retries → stays within 10 req/min
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def _fetch(self) -> dict[str, Any]:
        url = f"{self.BASE_URL}{self.ENDPOINT}"
        response = requests.get(
            url,
            params={"city": CITY, "key": self.api_key},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if str(payload.get("code")) != "200":
            raise ValueError(
                f"JuHe API error — code={payload.get('code')!r}, "
                f"msg={payload.get('msg')!r}. Check your API key."
            )
        return payload

    def _write_parquet(self, raw: dict[str, Any], path: Path) -> None:
        """Flatten the API response data fields into a single-row Parquet file."""
        data = raw.get("data", {})
        geo = data.get("geo", {})
        row = {
            "city": data.get("city"),
            "aqi": data.get("aqi"),
            "co": data.get("co"),
            "no2": data.get("no2"),
            "o3": data.get("o3"),
            "pm10": data.get("pm10"),
            "pm25": data.get("pm25"),
            "so2": data.get("so2"),
            "lat": geo.get("lat"),
            "lon": geo.get("lon"),
            "raw_response": json.dumps(raw),
        }
        pd.DataFrame([row]).to_parquet(path, index=False)
        logger.info("Landing Parquet written → %s", path)

    def _load_raw_from_parquet(self, path: Path) -> dict[str, Any]:
        """Reconstruct the original API response dict from a landing Parquet file."""
        df = pd.read_parquet(path)
        return json.loads(df["raw_response"].iloc[0])
