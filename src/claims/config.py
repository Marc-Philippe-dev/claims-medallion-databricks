"""Central configuration: Unity Catalog names, volume paths, business constants."""

from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Where the pipeline reads and writes. One schema per bundle target (dev/prod)."""

    catalog: str = "workspace"
    schema: str = "claims_dev"

    @property
    def landing(self) -> str:
        """Raw files land here (CSV, JSON lines, API responses)."""
        return f"/Volumes/{self.catalog}/{self.schema}/landing"

    @property
    def checkpoints(self) -> str:
        """Auto Loader state (checkpoints + inferred schemas). Never inside a table folder."""
        return f"/Volumes/{self.catalog}/{self.schema}/checkpoints"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"


def parse_config(argv: list[str] | None = None) -> Config:
    """Read --catalog / --schema passed by the job (see resources/claims_job.yml)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="claims_dev")
    args, _ = parser.parse_known_args(argv)
    return Config(catalog=args.catalog, schema=args.schema)


# Business constants -----------------------------------------------------------
CITIES = {  # city -> (latitude, longitude), used by the weather API
    "Rennes": (48.11, -1.68),
    "Nantes": (47.22, -1.55),
    "Brest": (48.39, -4.49),
}
CLAIM_TYPES = ("collision", "theft", "glass", "fire", "natural_event")
CLAIM_STATUSES = ("open", "closed", "rejected")
RAINY_DAY_THRESHOLD_MM = 1.0  # a day is "rainy" from 1 mm of precipitation
WEATHER_START = "2026-01-01"
WEATHER_END = "2026-06-30"
