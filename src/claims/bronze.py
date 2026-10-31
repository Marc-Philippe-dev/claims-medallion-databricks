"""Bronze layer: land raw files as-is in Delta tables with Auto Loader.

Bronze keeps everything (even bad rows) as strings, plus two lineage columns.
Auto Loader remembers which files it already processed (checkpoint), so a
second run ingests only new files: this is what makes the job idempotent.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_lineage(df: DataFrame) -> DataFrame:
    """Add where each row came from and when it was ingested."""
    return df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn(
        "_ingested_at", F.current_timestamp()
    )


def ingest_with_autoloader(
    spark,
    source_path: str,
    file_format: str,
    target_table: str,
    checkpoint_path: str,
    options: dict[str, str] | None = None,
) -> None:
    """Incrementally load new files from `source_path` into `target_table`.

    trigger(availableNow=True) = "process everything that is new, then stop".
    It is the only kind of streaming trigger supported on serverless compute.
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    )
    for key, value in (options or {}).items():
        reader = reader.option(key, value)

    query = (
        add_lineage(reader.load(source_path))
        .writeStream.option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()


# Source-specific options -------------------------------------------------------
CLAIMS_OPTIONS = {"header": "true", "cloudFiles.inferColumnTypes": "false"}
POLICIES_OPTIONS = {"cloudFiles.inferColumnTypes": "false"}
WEATHER_OPTIONS = {"multiLine": "true", "cloudFiles.inferColumnTypes": "true"}
