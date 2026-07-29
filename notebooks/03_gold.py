# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — model + incremental MERGE + OPTIMIZE/Z-ORDER
# MAGIC **Backs Jio #3 (dimensional modeling) + #1 (recurring daily).** The headline skills live here.

# COMMAND ----------

# TODO 1 — Build dimensions with SURROGATE keys:
#   dim_zone  (zone_sk, zone_id, borough, zone_name)   ← from the TLC zone lookup
#   dim_date  (date_sk, date, year, month, day, dow)
silver = spark.table("workspace.taxi.silver_yellow_trips")
silver.count()

# COMMAND ----------

# Just checking
%sql
Select distinct payment_type
from workspace.taxi.silver_yellow_trips
limit 10

# COMMAND ----------

from pyspark.sql.functions import col, year, month, dayofweek, to_date

silver = spark.table("workspace.taxi.silver_yellow_trips")

# --- FACT TABLE ---
# TODO: select the trip-level measures + foreign keys from silver.
# Decide: which columns are measures (numbers you'd sum/avg) and which are keys (point to dims)?
fact_trips = silver.select(
    "VendorID",
    "fare_amount","trip_distance","total_amount","passenger_count",
    "tpep_pickup_datetime","tpep_dropoff_datetime",
    "PULocationID","DOLocationID","payment_type","pickup_date","tip_amount"
)

# --- DIM DATE ---
# Build one row per distinct pickup_date with derived attributes.
dim_date = (silver.select("pickup_date").distinct()
    .withColumn("year", year(col("pickup_date")))
    .withColumn("month", month(col("pickup_date")))
    .withColumn("day_of_week", dayofweek(col("pickup_date"))))

# COMMAND ----------

from delta.tables import DeltaTable

# --- 1. Initial write of fact table (first run only) ---
(fact_trips.write
   .format("delta")
   .mode("overwrite")
   .partitionBy("pickup_date")
   .saveAsTable("workspace.taxi.fact_trips"))

# --- 2. Incremental MERGE upsert (the re-run / incremental pattern) ---
# On re-runs, this updates matching trips and inserts new ones — no duplicates.
fact_delta = DeltaTable.forName(spark, "workspace.taxi.fact_trips")

merge_condition = """
    t.VendorID = s.VendorID AND
    t.tpep_pickup_datetime = s.tpep_pickup_datetime AND
    t.tpep_dropoff_datetime = s.tpep_dropoff_datetime AND
    t.PULocationID = s.PULocationID AND
    t.DOLocationID = s.DOLocationID
"""

(fact_delta.alias("t")
   .merge(fact_trips.alias("s"), merge_condition)
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute())

# --- 3. Write dim_date ---
(dim_date.write
   .format("delta")
   .mode("overwrite")
   .saveAsTable("workspace.taxi.dim_date"))

# --- 4. OPTIMIZE + Z-ORDER on the high-cardinality filter column ---
spark.sql("OPTIMIZE workspace.taxi.fact_trips ZORDER BY (PULocationID)")

# COMMAND ----------

print("after first gold run:", spark.table("workspace.taxi.fact_trips").count())
# re-run block 2 (the MERGE) alone, then:
print("after second merge:", spark.table("workspace.taxi.fact_trips").count())

# COMMAND ----------

# TODO 5 — A couple of gold marts: daily revenue by zone, trips by hour, avg tip % by payment type.
#Pending Pending
from pyspark.sql.functions import col, sum, count, avg, hour, when

fact = spark.table("workspace.taxi.fact_trips")

# --- MART 1: daily revenue by zone ---
# Group by pickup_date + PULocationID, sum the revenue.
mart_daily_revenue = (fact
    .groupBy("pickup_date", "PULocationID")
    .agg(
        # TODO: sum of total_amount as "revenue", count of trips as "trip_count"
        (sum(col("total_amount")).alias("revenue")),
        (count(col("VendorID")).alias("trip_count" ))
    ))

# --- MART 2: trips by hour of day ---
# Extract the hour from pickup time, count trips per hour.
mart_trips_by_hour = (fact
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .groupBy("pickup_hour")
    .agg(
        # TODO: count of trips as "trip_count"
        (count(col("VendorId")).alias("trip_count"))
    ))

# --- MART 3: avg tip % by payment type ---
# This one needs a derived column first: tip as a % of fare.
# TODO: add "tip_pct" = tip_amount / fare_amount (guard against divide-by-zero),
#       then groupBy payment_type and avg it.


mart_tip_by_payment = (fact
    .withColumn("tip_pct",
        when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount"))
        .otherwise(None))
    .groupBy("payment_type")
    .agg(avg("tip_pct").alias("avg_tip_pct")))

# COMMAND ----------

display(mart_tip_by_payment.show(5))
display(mart_trips_by_hour.show(5))
display(mart_daily_revenue.show(5))
