"""Third source: daily precipitation from the Open-Meteo archive API (no API key).

The API response is first landed as a JSON file in the Volume (raw, untouched),
then read by Auto Loader like any other file. Landing raw API responses first
means we can always replay the pipeline without calling the API again.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from claims.config import RAINY_DAY_THRESHOLD_MM

API_URL = "https://archive-api.open-meteo.com/v1/archive"


def build_url(latitude: float, longitude: float, start: str, end: str) -> str:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start,
        "end_date": end,
        "daily": "precipitation_sum",
        "timezone": "Europe/Paris",
    }
    return f"{API_URL}?{urllib.parse.urlencode(params)}"


def fetch_city_weather(
    city: str, latitude: float, longitude: float, start: str, end: str
) -> dict:
    """Call the API and wrap the raw response with the city name and fetch time."""
    with urllib.request.urlopen(
        build_url(latitude, longitude, start, end), timeout=30
    ) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return {
        "city": city,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def flatten_weather(bronze_weather: DataFrame) -> DataFrame:
    """One row per (city, day) from the nested API response.

    payload.daily.time              = ["2026-01-01", "2026-01-02", ...]
    payload.daily.precipitation_sum = [3.2, 0.0, ...]
    The two arrays are aligned by position, so we explode one with its index.
    """
    exploded = bronze_weather.select(
        "city",
        "fetched_at",
        F.posexplode("payload.daily.time").alias("pos", "day"),
        F.col("payload.daily.precipitation_sum").alias("precip_array"),
    )
    daily = exploded.select(
        F.col("city"),
        F.to_date("day").alias("weather_date"),
        F.expr("CAST(precip_array[pos] AS DOUBLE)").alias("precipitation_mm"),
        F.col("fetched_at"),
    )
    # The same city can be fetched several times: keep the latest value per day.
    latest = daily.groupBy("city", "weather_date").agg(
        F.max_by("precipitation_mm", "fetched_at").alias("precipitation_mm")
    )
    return latest.withColumn(
        "is_rainy_day",
        F.coalesce(F.col("precipitation_mm"), F.lit(0.0)) >= RAINY_DAY_THRESHOLD_MM,
    )
