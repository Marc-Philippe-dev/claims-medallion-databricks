"""One way to get a SparkSession, wherever the code runs."""

from __future__ import annotations


def get_spark():
    """Return a SparkSession.

    - Locally (VS Code, pytest): Databricks Connect sends the work to serverless
      compute. Serverless is selected by `serverless_compute_id = auto` in
      ~/.databrickscfg or by DATABRICKS_SERVERLESS_COMPUTE_ID=auto.
    - Inside a Databricks job: the same call returns the job's own session.
    - Without Databricks Connect installed (plain pyspark): local Spark, used
      only to run the unit tests offline.
    """
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        from pyspark.sql import SparkSession

        return (
            SparkSession.builder.master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "Europe/Paris")
            .getOrCreate()
        )
