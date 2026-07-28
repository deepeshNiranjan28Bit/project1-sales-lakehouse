# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — clean &amp; conform
# MAGIC **Backs Jio #1 (standardize) + #4 (validation).**
# MAGIC
# MAGIC Import your PURE functions from `src/transforms/cleaning.py` so the logic is unit-tested in CI,
# MAGIC not buried in a notebook. (In Databricks Repos you can `from src.transforms.cleaning import ...`.)

# COMMAND ----------

# TODO 1 — Read the bronze table:  df = spark.read.table("bronze_yellow_trips")
bronze = spark.table("workspace.taxi.bronze_yellow_trips")
bronze.printSchema()

# COMMAND ----------

# TODO 2 — Apply cleaning (use src.transforms.cleaning), logging row counts IN and OUT of each step:
#   - filter_valid_trips  (drop negative fares, zero passengers, bad coords, dropoff < pickup)
#   - cast_types
#   - deduplicate on the trip key
#   - add_pickup_date
from pyspark.sql.functions import col, lit, coalesce

bronze = spark.table("workspace.taxi.bronze_yellow_trips")

# Define the "valid row" condition — combine your rules with &
valid_condition = (
    # rule 1: pickup date in range
    (col("pickup_date") >= "2023-01-01") & (col("pickup_date") < "2023-04-01") &
    # rule 2: dropoff after pickup
    (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
    # rule 3: fare and total non-negative
    (col("fare_amount") >= 0) & (col("total_amount") >= 0) &
    # rule 4: distance non-negative, passengers positive
    (col("trip_distance") >= 0) & (col("passenger_count") > 0)
)
valid_strict = coalesce(valid_condition, lit(False))

silver_clean = bronze.filter(valid_strict)
silver_quarantine = bronze.filter(~valid_strict)

# COMMAND ----------

# TODO 3 — Quarantine, don't silently drop: write rejected rows to a `silver_yellow_trips_rejects`
#   table so nothing disappears without a trace. (Interviewers love this.)
b = bronze.count()
c = silver_clean.count()
q = silver_quarantine.count()
print("bronze:", b, "| clean:", c, "| quarantine:", q, "| reconciles:", b == c + q)

# COMMAND ----------

from pyspark.sql.functions import col

# Define the trip key: columns that together uniquely identify one trip
trip_key = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
]

# Deduplicate the clean rows on that key
silver_clean_deduped = silver_clean.dropDuplicates(trip_key)

# COMMAND ----------

before = silver_clean.count()
after = silver_clean_deduped.count()
print("before dedup:", before, "| after:", after, "| dupes removed:", before - after)

# COMMAND ----------

(silver_clean_deduped.write
   .format("delta")
   .mode("overwrite")
   .partitionBy("pickup_date")
   .saveAsTable("workspace.taxi.silver_yellow_trips"))

# COMMAND ----------

(silver_quarantine.write
   .format("delta")
   .mode("overwrite")
   .partitionBy("pickup_date")
   .saveAsTable("workspace.taxi.silver_yellow_trips_quarantine"))

# COMMAND ----------

b = spark.table("workspace.taxi.bronze_yellow_trips").count()
c = spark.table("workspace.taxi.silver_yellow_trips").count()
q = spark.table("workspace.taxi.silver_yellow_trips_quarantine").count()
dupes = 18   # from your dedup step

print("bronze:", b, "| clean:", c, "| quarantine:", q, "| dupes:", dupes)
print("reconciles:", b == c + q + dupes)