"""Silver layer: typed, deduplicated, validated data + a quarantine table.

Every function here is pure (DataFrame in -> DataFrame out, no read, no write),
which is what makes them easy to unit test with a tiny hand-made DataFrame.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from claims.config import CLAIM_STATUSES, CLAIM_TYPES


# Claims -----------------------------------------------------------------------
def cast_claims(bronze_claims: DataFrame) -> DataFrame:
    """Strings -> proper types. Values that cannot be converted become NULL
    (try_cast / try_to_timestamp) instead of crashing the job; the quality rules
    below then send those rows to quarantine."""
    return bronze_claims.select(
        F.trim("claim_id").alias("claim_id"),
        F.trim("policy_id").alias("policy_id"),
        F.col("claim_date").alias("claim_date_raw"),
        F.expr("CAST(try_to_timestamp(trim(claim_date), 'yyyy-MM-dd') AS DATE)").alias(
            "claim_date"
        ),
        F.col("claim_amount").alias("claim_amount_raw"),
        F.expr("try_cast(trim(claim_amount) AS DECIMAL(12,2))").alias("claim_amount"),
        F.lower(F.trim("claim_type")).alias("claim_type"),
        F.lower(F.trim("status")).alias("status"),
        "_source_file",
        "_ingested_at",
    )


def deduplicate_claims(claims: DataFrame) -> DataFrame:
    """Keep one row per claim_id: the most recently ingested one.
    A claim re-sent in a later file is a duplicate, not an error."""
    window = Window.partitionBy("claim_id").orderBy(
        F.col("_ingested_at").desc(), F.col("_source_file").desc()
    )
    return (
        claims.withColumn("_rn", F.row_number().over(window))
        .filter((F.col("_rn") == 1) | F.col("claim_id").isNull())
        .drop("_rn")
    )


def add_quality_reason(
    claims: DataFrame, policies: DataFrame, as_of: date | None = None
) -> DataFrame:
    """Add `dq_reason`: NULL when the row is valid, otherwise the broken rules
    separated by ';' (a row can break several rules)."""
    today = F.lit(as_of) if as_of else F.current_date()
    known_policies = policies.select(
        F.col("policy_id").alias("_known_policy_id")
    ).distinct()
    joined = claims.join(
        known_policies,
        claims["policy_id"] == known_policies["_known_policy_id"],
        "left",
    )

    rules = [
        ("missing_claim_id", F.col("claim_id").isNull() | (F.col("claim_id") == "")),
        ("invalid_date", F.col("claim_date").isNull()),
        ("future_date", F.col("claim_date") > today),
        ("invalid_amount", F.col("claim_amount").isNull()),
        ("negative_amount", F.col("claim_amount") < 0),
        ("unknown_claim_type", ~F.col("claim_type").isin(*CLAIM_TYPES)),
        ("unknown_status", ~F.col("status").isin(*CLAIM_STATUSES)),
        ("unknown_policy", F.col("_known_policy_id").isNull()),
    ]
    # when(...) without otherwise() gives NULL, and concat_ws skips NULLs.
    reasons = F.concat_ws(
        ";",
        *[F.when(F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules],
    )
    return joined.withColumn(
        "dq_reason", F.when(reasons == "", F.lit(None)).otherwise(reasons)
    ).drop("_known_policy_id")


def split_valid_invalid(checked: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Valid rows -> silver_claims ; invalid rows -> quarantine_claims."""
    valid = checked.filter(F.col("dq_reason").isNull()).drop(
        "dq_reason", "claim_date_raw", "claim_amount_raw"
    )
    invalid = checked.filter(F.col("dq_reason").isNotNull()).withColumn(
        "quarantined_at", F.current_timestamp()
    )
    return valid, invalid


# Policies ---------------------------------------------------------------------
def clean_policies(bronze_policies: DataFrame) -> DataFrame:
    """Type the policy reference data and keep one row per policy_id."""
    typed = bronze_policies.select(
        F.trim("policy_id").alias("policy_id"),
        F.initcap(F.trim("city")).alias("city"),
        F.lower(F.trim("vehicle_type")).alias("vehicle_type"),
        F.expr("CAST(try_to_timestamp(trim(start_date), 'yyyy-MM-dd') AS DATE)").alias(
            "start_date"
        ),
        F.expr("try_cast(annual_premium AS DECIMAL(10,2))").alias("annual_premium"),
        "_ingested_at",
    )
    window = Window.partitionBy("policy_id").orderBy(F.col("_ingested_at").desc())
    return (
        typed.filter(F.col("policy_id").isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .filter("_rn = 1")
        .drop("_rn", "_ingested_at")
    )
