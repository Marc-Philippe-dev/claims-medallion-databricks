"""Task 2 - call the Open-Meteo API and land one raw JSON file per city in the Volume.

Works both locally (your PC calls the API, the SDK uploads to the Volume) and
inside the job (needs outbound internet on Free Edition: LinkedIn verification).
"""

import io
import json
import os
import sys
from pathlib import Path

# Make src/ importable (the bundle uploads the whole repo, src/ sits next to jobs/).
try:
    _ROOT = Path(__file__).resolve().parents[1]
except NameError:  # safety net if a runtime does not define __file__
    _ROOT = Path(os.getcwd()).resolve().parent
sys.path.insert(0, str(_ROOT / "src"))

from databricks.sdk import WorkspaceClient

from claims.config import CITIES, WEATHER_END, WEATHER_START, parse_config
from claims.weather import fetch_city_weather


def main() -> None:
    cfg = parse_config()
    client = WorkspaceClient()  # same auth as the CLI (profile or job identity)
    for city, (lat, lon) in CITIES.items():
        record = fetch_city_weather(city, lat, lon, WEATHER_START, WEATHER_END)
        target = f"{cfg.landing}/weather/weather_{city.lower()}_{WEATHER_START}_{WEATHER_END}.json"
        client.files.upload(
            target, io.BytesIO(json.dumps(record).encode("utf-8")), overwrite=True
        )
        days = len(record["payload"]["daily"]["time"])
        print(f"{city}: {days} days -> {target}")


if __name__ == "__main__":
    main()
