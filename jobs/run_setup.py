"""Task 1 - create the schema and the two volumes (idempotent: safe to re-run)."""

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
from claims.spark_session import get_spark


def main() -> None:
    cfg = parse_config()
    spark = get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
    for volume in ("landing", "checkpoints"):
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{volume}")
    print(
        f"Ready: schema {cfg.catalog}.{cfg.schema}, volumes {cfg.landing} and {cfg.checkpoints}"
    )


if __name__ == "__main__":
    main()
