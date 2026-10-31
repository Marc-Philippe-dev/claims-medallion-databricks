"""Task 4 - Silver: types, deduplication, quality rules, quarantine."""

import os
import sys
from pathlib import Path

# Make src/ importable (the bundle uploads the whole repo, src/ sits next to jobs/).
try:
    _ROOT = Path(__file__).resolve().parents[1]
except NameError:  # safety net if a runtime does not define __file__
    _ROOT = Path(os.getcwd()).resolve().parent
sys.path.insert(0, str(_ROOT / "src"))

from claims.config import parse_config
from claims.silver import (
    add_quality_reason,
    cast_claims,
    clean_policies,
    deduplicate_claims,
    split_valid_invalid,
)
from claims.spark_session import get_spark
from claims.weather import flatten_weather


def overwrite(df, table: str) -> None:
    """Full refresh: Silver is rebuilt from Bronze at each run (simple and safe)."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def main() -> None:
    cfg = parse_config()
    spark = get_spark()

    policies = clean_policies(spark.table(cfg.table("bronze_policies")))
    overwrite(policies, cfg.table("silver_policies"))
    policies = spark.table(cfg.table("silver_policies"))

    claims = deduplicate_claims(cast_claims(spark.table(cfg.table("bronze_claims"))))
    valid, invalid = split_valid_invalid(add_quality_reason(claims, policies))
    overwrite(valid, cfg.table("silver_claims"))
    overwrite(invalid, cfg.table("quarantine_claims"))

    overwrite(
        flatten_weather(spark.table(cfg.table("bronze_weather"))),
        cfg.table("silver_weather_daily"),
    )

    for table in (
        "silver_policies",
        "silver_claims",
        "quarantine_claims",
        "silver_weather_daily",
    ):
        print(f"{table}: {spark.table(cfg.table(table)).count()} rows")


if __name__ == "__main__":
    main()
