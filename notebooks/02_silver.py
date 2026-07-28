# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — clean &amp; conform
# MAGIC **Backs Jio #1 (standardize) + #4 (validation).**
# MAGIC
# MAGIC Import your PURE functions from `src/transforms/cleaning.py` so the logic is unit-tested in CI,
# MAGIC not buried in a notebook. (In Databricks Repos you can `from src.transforms.cleaning import ...`.)

# COMMAND ----------
# TODO 1 — Read the bronze table:  df = spark.read.table("bronze_yellow_trips")

# COMMAND ----------
# TODO 2 — Apply cleaning (use src.transforms.cleaning), logging row counts IN and OUT of each step:
#   - filter_valid_trips  (drop negative fares, zero passengers, bad coords, dropoff < pickup)
#   - cast_types
#   - deduplicate on the trip key
#   - add_pickup_date

# COMMAND ----------
# TODO 3 — Quarantine, don't silently drop: write rejected rows to a `silver_yellow_trips_rejects`
#   table so nothing disappears without a trace. (Interviewers love this.)

# COMMAND ----------
# TODO 4 — Write SILVER Delta, partitioned by pickup_date.
#   Make it idempotent: re-running the same input produces the same silver table.
