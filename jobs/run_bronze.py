"""Task 3 - Bronze: Auto Loader ingestion of the three sources."""

import os
import sys
from pathlib import Path

# Make src/ importable (the bundle uploads the whole repo, src/ sits next to jobs/).
try:
    _ROOT = Path(__file__).resolve().parents[1]
except NameError:  # safety net if a runtime does not define __file__
    _ROOT = Path(os.getcwd()).resolve().parent
sys.path.insert(0, str(_ROOT / "src"))

from claims.bronze import (
    CLAIMS_OPTIONS,
    POLICIES_OPTIONS,
    WEATHER_OPTIONS,
    ingest_with_autoloader,
)
from claims.config import parse_config
from claims.spark_session import get_spark

SOURCES = [
    # (folder in landing, format, bronze table, options)
    ("claims", "csv", "bronze_claims", CLAIMS_OPTIONS),
    ("policies", "json", "bronze_policies", POLICIES_OPTIONS),
    ("weather", "json", "bronze_weather", WEATHER_OPTIONS),
]


def main() -> None:
    cfg = parse_config()
    spark = get_spark()
    for folder, fmt, table, options in SOURCES:
        ingest_with_autoloader(
            spark,
            source_path=f"{cfg.landing}/{folder}/",
            file_format=fmt,
            target_table=cfg.table(table),
            checkpoint_path=f"{cfg.checkpoints}/{table}",
            options=options,
        )
        count = spark.table(cfg.table(table)).count()
        print(f"{table}: {count} rows")


if __name__ == "__main__":
    main()
