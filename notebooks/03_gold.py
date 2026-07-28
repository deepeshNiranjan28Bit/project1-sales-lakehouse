# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — model + incremental MERGE + OPTIMIZE/Z-ORDER
# MAGIC **Backs Jio #3 (dimensional modeling) + #1 (recurring daily).** The headline skills live here.

# COMMAND ----------
# TODO 1 — Build dimensions with SURROGATE keys:
#   dim_zone  (zone_sk, zone_id, borough, zone_name)   ← from the TLC zone lookup
#   dim_date  (date_sk, date, year, month, day, dow)

# COMMAND ----------
# TODO 2 — Build fact_trips. Grain = ONE ROW PER TRIP. Measures: fare, tip, distance, total.
#   Fact holds the surrogate FKs (zone_sk, date_sk), not the natural keys.
#   Write the heavier aggregations in Spark SQL: spark.sql("SELECT ... FROM ...").

# COMMAND ----------
# TODO 3 — INCREMENTAL UPSERT with Delta MERGE (the standout skill). Simulate "a new day":
#   MERGE INTO gold_fact_trips t USING updates s ON t.trip_key = s.trip_key
#   WHEN MATCHED THEN UPDATE SET *
#   WHEN NOT MATCHED THEN INSERT *
#   Be ready to explain matched / not-matched clauses and why this is idempotent.

# COMMAND ----------
# TODO 4 — Performance: OPTIMIZE gold_fact_trips; then ZORDER BY (zone_id, pickup_date).
#   Note the file count / data-skipping benefit before vs after.

# COMMAND ----------
# TODO 5 — A couple of gold marts: daily revenue by zone, trips by hour, avg tip % by payment type.
