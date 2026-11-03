"""Unit tests for the Gold aggregates and the weather flattening."""

from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from claims.gold import claims_by_weather, claims_monthly_kpis, quality_summary
from claims.weather import flatten_weather


def _silver_claims(spark):
    rows = [
        ("CLM-1", "POL-1", date(2026, 1, 10), Decimal("1000.00"), "collision"),
        ("CLM-2", "POL-1", date(2026, 1, 11), Decimal("3000.00"), "collision"),
        ("CLM-3", "POL-2", date(2026, 1, 11), Decimal("200.00"), "glass"),
        ("CLM-4", "POL-2", date(2026, 2, 3), Decimal("400.00"), "glass"),
    ]
    return spark.createDataFrame(
        rows,
        "claim_id string, policy_id string, claim_date date, claim_amount decimal(12,2), claim_type string",
    )


def test_monthly_kpis(spark):
    result = {
        (r.claim_month, r.claim_type): r
        for r in claims_monthly_kpis(_silver_claims(spark)).collect()
    }
    jan_collision = result[(date(2026, 1, 1), "collision")]
    assert jan_collision.claim_count == 3
    assert jan_collision.total_amount == Decimal("4000.00")
    assert jan_collision.avg_amount == Decimal("2000.00")
    assert result[(date(2026, 2, 1), "glass")].claim_count == 1


def test_claims_by_weather_divides_by_number_of_days(spark):
    policies = spark.createDataFrame(
        [("POL-1", "Rennes"), ("POL-2", "Rennes")], ["policy_id", "city"]
    )
    weather = spark.createDataFrame(
        [
            ("Rennes", date(2026, 1, 10), 0.0, False),
            ("Rennes", date(2026, 1, 11), 5.2, True),
            ("Rennes", date(2026, 1, 12), 0.0, False),
            ("Rennes", date(2026, 2, 3), 0.0, False),
        ],
        "city string, weather_date date, precipitation_mm double, is_rainy_day boolean",
    )
    rows = {
        r.is_rainy_day: r
        for r in claims_by_weather(_silver_claims(spark), policies, weather).collect()
    }
    assert (
        rows[True].claim_count,
        rows[True].day_count,
        rows[True].claims_per_day,
    ) == (2, 1, 2.0)
    assert (
        rows[False].claim_count,
        rows[False].day_count,
        rows[False].claims_per_day,
    ) == (2, 3, 0.67)


def test_quality_summary_splits_multiple_reasons(spark):
    quarantine = spark.createDataFrame(
        [("invalid_date",), ("invalid_date;unknown_policy",), ("negative_amount",)],
        ["dq_reason"],
    )
    result = {r.reason: r.row_count for r in quality_summary(quarantine).collect()}
    assert result == {"invalid_date": 2, "unknown_policy": 1, "negative_amount": 1}


def test_flatten_weather_one_row_per_day(spark):
    schema = StructType(
        [
            StructField("city", StringType()),
            StructField("fetched_at", StringType()),
            StructField(
                "payload",
                StructType(
                    [
                        StructField(
                            "daily",
                            StructType(
                                [
                                    StructField("time", ArrayType(StringType())),
                                    StructField(
                                        "precipitation_sum", ArrayType(DoubleType())
                                    ),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
        ]
    )
    bronze = spark.createDataFrame(
        [
            (
                "Brest",
                "2026-07-01T00:00:00",
                {
                    "daily": {
                        "time": ["2026-01-01", "2026-01-02"],
                        "precipitation_sum": [4.5, 0.2],
                    }
                },
            )
        ],
        schema,
    )
    rows = {r.weather_date: r for r in flatten_weather(bronze).collect()}
    assert len(rows) == 2
    assert rows[date(2026, 1, 1)].is_rainy_day is True
    assert rows[date(2026, 1, 2)].is_rainy_day is False
