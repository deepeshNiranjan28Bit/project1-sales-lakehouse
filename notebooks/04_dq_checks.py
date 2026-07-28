# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Data-Quality gates
# MAGIC **Backs Jio #4.** A failing check must FAIL the job (raise) — not just log a warning.

# COMMAND ----------
# TODO — import src.dq.checks and run the gates, e.g.:
#   assert_unique(dim_zone, ["zone_sk"])                                  # PK uniqueness
#   assert_not_null(fact_trips, ["trip_key", "fare_amount"])             # required fields
#   assert_referential_integrity(fact_trips, dim_zone, "zone_sk", "zone_sk")  # anti-join == 0
#   assert_row_count_between(fact_trips, expected_low, expected_high)    # volume sanity

# COMMAND ----------
# TODO — Record results: write pass/fail + timestamp to a `dq_results` Delta table,
#   then raise on ANY failure so the Workflow marks the run failed.
