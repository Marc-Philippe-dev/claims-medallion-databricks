"""Unit tests for the Silver transformations."""

from datetime import date
from decimal import Decimal

from claims.silver import (
    add_quality_reason,
    cast_claims,
    clean_policies,
    deduplicate_claims,
    split_valid_invalid,
)

AS_OF = date(2026, 7, 1)


def _checked(bronze_claims, silver_policies):
    claims = deduplicate_claims(cast_claims(bronze_claims))
    return add_quality_reason(claims, silver_policies, as_of=AS_OF)


def test_cast_turns_invalid_dates_into_null(bronze_claims):
    rows = {r.claim_id: r for r in cast_claims(bronze_claims).collect()}
    assert rows["CLM-2"].claim_date is None  # 30 February does not exist
    assert rows["CLM-3"].claim_date is None  # wrong format dd/MM/yyyy
    assert rows["CLM-4"].claim_amount == Decimal("-50.00")


def test_cast_normalises_text_values(bronze_claims):
    row = next(r for r in cast_claims(bronze_claims).collect() if r.claim_id == "CLM-6")
    assert (row.claim_type, row.status) == ("glass", "closed")


def test_deduplicate_keeps_latest_ingestion(bronze_claims):
    deduped = deduplicate_claims(cast_claims(bronze_claims))
    clm1 = deduped.filter("claim_id = 'CLM-1'").collect()
    assert len(clm1) == 1
    assert clm1[0]._source_file == "f2.csv"
    assert deduped.count() == 6


def test_quality_rules_give_the_expected_reasons(bronze_claims, silver_policies):
    reasons = {
        r.claim_id: r.dq_reason
        for r in _checked(bronze_claims, silver_policies).collect()
    }
    assert reasons == {
        "CLM-1": None,
        "CLM-2": "invalid_date",
        "CLM-3": "invalid_date",
        "CLM-4": "negative_amount",
        "CLM-5": "unknown_policy",
        "CLM-6": None,
    }


def test_split_sends_invalid_rows_to_quarantine(bronze_claims, silver_policies):
    valid, invalid = split_valid_invalid(_checked(bronze_claims, silver_policies))
    assert valid.count() == 2
    assert invalid.count() == 4
    assert "dq_reason" not in valid.columns
    assert {"dq_reason", "claim_date_raw", "quarantined_at"} <= set(invalid.columns)


def test_clean_policies_deduplicates(spark):
    from pyspark.sql import functions as F

    raw = spark.createDataFrame(
        [
            ("POL-1", "rennes", "SUV", "2021-01-01", "500.00", "2026-07-01 10:00:00"),
            ("POL-1", "rennes", "SUV", "2021-01-01", "550.00", "2026-07-02 10:00:00"),
        ],
        [
            "policy_id",
            "city",
            "vehicle_type",
            "start_date",
            "annual_premium",
            "_ingested_at",
        ],
    ).withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    rows = clean_policies(raw).collect()
    assert len(rows) == 1
    assert rows[0].city == "Rennes"
    assert rows[0].annual_premium == Decimal("550.00")
