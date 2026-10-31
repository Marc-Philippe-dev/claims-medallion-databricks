"""Gold layer: business-ready aggregates, one table per question."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def claims_monthly_kpis(silver_claims: DataFrame) -> DataFrame:
    """How many claims and how much do they cost, per month and per type?"""
    return (
        silver_claims.withColumn("claim_month", F.trunc("claim_date", "month"))
        .groupBy("claim_month", "claim_type")
        .agg(
            F.count("*").alias("claim_count"),
            F.sum("claim_amount").cast("decimal(14,2)").alias("total_amount"),
            F.avg("claim_amount").cast("decimal(12,2)").alias("avg_amount"),
        )
        .orderBy("claim_month", "claim_type")
    )


def claims_by_weather(
    silver_claims: DataFrame, policies: DataFrame, weather: DataFrame
) -> DataFrame:
    """Do we get more claims on rainy days? Claims per day, per city, rainy vs dry.

    We divide by the number of rainy/dry days, otherwise a city with more dry
    days would always look like it has more 'dry' claims.
    """
    day_counts = weather.groupBy("city", "is_rainy_day").agg(
        F.countDistinct("weather_date").alias("day_count")
    )
    weather_by_day = weather.withColumnRenamed("weather_date", "claim_date")
    claims_per_group = (
        silver_claims.join(policies.select("policy_id", "city"), "policy_id")
        .join(weather_by_day, ["city", "claim_date"])
        .groupBy("city", "is_rainy_day")
        .agg(F.count("*").alias("claim_count"))
    )
    return (
        day_counts.join(claims_per_group, ["city", "is_rainy_day"], "left")
        .fillna(0, subset=["claim_count"])
        .withColumn(
            "claims_per_day", F.round(F.col("claim_count") / F.col("day_count"), 2)
        )
        .orderBy("city", "is_rainy_day")
    )


def quality_summary(quarantine: DataFrame) -> DataFrame:
    """How many rows were rejected, for which reason? (a row can have several)."""
    return (
        quarantine.withColumn("reason", F.explode(F.split("dq_reason", ";")))
        .groupBy("reason")
        .agg(F.count("*").alias("row_count"))
        .orderBy(F.col("row_count").desc(), "reason")
    )
