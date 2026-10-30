"""Step 1 smoke test: is Databricks Connect talking to serverless compute?

Run:  python scripts/check_connection.py
"""

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

print("Spark version on the server:", spark.version)
spark.range(5).withColumnRenamed("id", "n").show()
spark.sql("SELECT current_catalog() AS catalog, current_user() AS me").show(truncate=False)
