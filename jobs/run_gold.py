"""Task 5 - Gold: business KPIs."""

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
from claims.gold import claims_by_weather, claims_monthly_kpis, quality_summary
from claims.spark_session import get_spark


def overwrite(df, table: str) -> None:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def main() -> None:
    cfg = parse_config()
    spark = get_spark()
    claims = spark.table(cfg.table("silver_claims"))
    policies = spark.table(cfg.table("silver_policies"))
    weather = spark.table(cfg.table("silver_weather_daily"))
    quarantine = spark.table(cfg.table("quarantine_claims"))

    overwrite(claims_monthly_kpis(claims), cfg.table("gold_claims_monthly"))
    overwrite(
        claims_by_weather(claims, policies, weather), cfg.table("gold_claims_weather")
    )
    overwrite(quality_summary(quarantine), cfg.table("gold_quality_summary"))

    spark.table(cfg.table("gold_claims_monthly")).show(12, truncate=False)
    spark.table(cfg.table("gold_claims_weather")).show(truncate=False)
    spark.table(cfg.table("gold_quality_summary")).show(truncate=False)


if __name__ == "__main__":
    main()
