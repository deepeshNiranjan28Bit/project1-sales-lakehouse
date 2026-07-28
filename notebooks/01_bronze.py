# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — land raw NYC taxi data as Delta (untouched)
# MAGIC **Backs Jio bullet #1** — standardize raw feeds into query-ready datasets, recurring daily.
# MAGIC
# MAGIC **Rule:** bronze = raw. NO cleaning here. Land it, add ingest metadata, partition. That's it.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO 1 — Get the data into Databricks.
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.taxi;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.taxi.raw;

# COMMAND ----------

import urllib.request

base = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
months = ["2023-01", "2023-02", "2023-03"]
for m in months:
    fname = f"yellow_tripdata_{m}.parquet"
    url = base + fname
    dest = f"/Volumes/workspace/taxi/raw/{fname}"
    urllib.request.urlretrieve(url, dest)
    print("downloaded", dest)

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/taxi/raw/"))

# COMMAND ----------

df_peek = spark.read.parquet("/Volumes/workspace/taxi/raw/yellow_tripdata_2023-01.parquet")
df_peek.printSchema()

# COMMAND ----------

# TODO 2 — Read parquet with an EXPLICIT schema. Do NOT inferSchema in a pipeline.
#   Define taxi_schema = StructType([...]) first, then:
#   df_raw = spark.read.schema(taxi_schema).parquet(<volume_path>)
from pyspark.sql.functions import col, current_timestamp, to_date, lit
from functools import reduce

months = ["2023-01", "2023-02", "2023-03"]

def read_and_normalize(m):
    d = spark.read.parquet(f"/Volumes/workspace/taxi/raw/yellow_tripdata_{m}.parquet")
    d = d.withColumn("source_file", lit(f"yellow_tripdata_{m}.parquet"))
    d = d.withColumnRenamed("Airport_fee", "airport_fee")
    d = (d
         .withColumn("airport_fee", col("airport_fee").cast("double"))
         .withColumn("VendorID", col("VendorID").cast("long"))
         .withColumn("passenger_count", col("passenger_count").cast("double"))
         .withColumn("RatecodeID", col("RatecodeID").cast("double"))
         .withColumn("PULocationID", col("PULocationID").cast("long"))
         .withColumn("DOLocationID", col("DOLocationID").cast("long")))
    return d

dfs = [read_and_normalize(m) for m in months]
df = reduce(lambda a, b: a.unionByName(b), dfs)

df = (df
      .withColumn("ingest_ts", current_timestamp())
      .withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))))

print("columns:", len(df.columns))
display(df.limit(10))


# COMMAND ----------

# TODO 3 — Add ingest metadata: ingest_ts (current_timestamp), source_file (input_file_name()).
#   Derive pickup_date (DateType) from the pickup timestamp for partitioning.
#write that df as the bronze Delta table. This is the moment your files-plus-lineage DataFrame becomes an actual queryable table on disk
(df.write
   .format("delta")
   .mode("overwrite")
   .partitionBy("pickup_date")
   .saveAsTable("workspace.taxi.bronze_yellow_trips"))

# COMMAND ----------

# TODO 4 — Write the BRONZE Delta table, partitioned by pickup_date, append mode.
#   df_bronze.write.format("delta").mode("append").partitionBy("pickup_date") \
#            .saveAsTable("bronze_yellow_trips")

# COMMAND ----------

# TODO 5 — Sanity log: print the bronze row count. It should equal the source row count.
#verify the counts match
table_count = spark.table("workspace.taxi.bronze_yellow_trips").count()
source_count = df.count()
print("table:", table_count, "| source:", source_count, "| match:", table_count == source_count)