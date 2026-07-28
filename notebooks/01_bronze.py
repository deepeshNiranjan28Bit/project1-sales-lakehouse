# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — land raw NYC taxi data as Delta (untouched)
# MAGIC **Backs Jio bullet #1** — standardize raw feeds into query-ready datasets, recurring daily.
# MAGIC
# MAGIC **Rule:** bronze = raw. NO cleaning here. Land it, add ingest metadata, partition. That's it.

# COMMAND ----------
# TODO 1 — Get the data into Databricks.
#   Recommended on Free Edition: create a Unity Catalog Volume, then in Python loop-download
#   the monthly files into it, then read with Spark.
#   URL pattern (verified live):
#     https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
#     ... 2023-01 through 2023-12 (~50 MB each, ~40M rows total for the year).

# COMMAND ----------
# TODO 2 — Read parquet with an EXPLICIT schema. Do NOT inferSchema in a pipeline.
#   Define taxi_schema = StructType([...]) first, then:
#   df_raw = spark.read.schema(taxi_schema).parquet(<volume_path>)

# COMMAND ----------
# TODO 3 — Add ingest metadata: ingest_ts (current_timestamp), source_file (input_file_name()).
#   Derive pickup_date (DateType) from the pickup timestamp for partitioning.

# COMMAND ----------
# TODO 4 — Write the BRONZE Delta table, partitioned by pickup_date, append mode.
#   df_bronze.write.format("delta").mode("append").partitionBy("pickup_date") \
#            .saveAsTable("bronze_yellow_trips")

# COMMAND ----------
# TODO 5 — Sanity log: print the bronze row count. It should equal the source row count.
