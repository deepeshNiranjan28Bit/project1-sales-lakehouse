# Architecture &amp; Design Decisions

## Medallion flow

```
NYC TLC Yellow Taxi parquet (public CloudFront)
        │  PySpark read (explicit schema)
        ▼
   BRONZE Delta      raw, untouched, + ingest_ts, partitioned by pickup_date
        │  clean · cast · dedupe · validate  (src/transforms/cleaning.py)
        ▼
   SILVER Delta      conformed rows; rejects quarantined to silver/_rejects
        │  aggregate · model
        ▼
   GOLD Delta        fact_trips  +  dim_zone  +  dim_date
                     • Delta MERGE  (daily incremental upsert)
                     • OPTIMIZE + ZORDER BY (zone_id, pickup_date)
                     • SQL data-quality gates  (src/dq/checks.py)

Databricks Workflow:  bronze → silver → gold → dq   (dependent tasks, scheduled daily)
```

## Design decisions — fill these in as you build (this is what senior interviewers read)

- **Why Delta over plain parquet?** ACID, time travel, schema enforcement, MERGE. _TODO: expand._
- **Bronze = raw.** No cleaning in bronze; it's the replayable source of truth. _TODO: why this matters._
- **Gold grain.** One row per ______. Measures: ______ (additive vs non-additive?). _TODO._
- **Partition vs Z-ORDER.** Partition on low-cardinality (`pickup_date`); Z-ORDER on high-cardinality
  filter columns (`zone_id`). _TODO: explain data-skipping / small-file problem._
- **MERGE semantics.** `WHEN MATCHED → UPDATE`, `WHEN NOT MATCHED → INSERT`; re-running is idempotent. _TODO._
- **DQ gating.** A failed check RAISES and fails the job — not a warning. _TODO: list the checks._
```
