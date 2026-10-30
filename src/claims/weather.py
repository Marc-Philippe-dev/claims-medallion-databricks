"""Third source: daily precipitation from the Open-Meteo archive API (no API key).

The API response is first landed as a JSON file in the Volume (raw, untouched),
then read by Auto Loader like any other file. Landing raw API responses first
means we can always replay the pipeline without calling the API again.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone, UTC

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
        "fetched_at": datetime.now(UTC).isoformat(),
        "payload": payload,
    }
