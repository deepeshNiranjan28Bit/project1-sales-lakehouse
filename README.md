# Project 1 — Sales Lakehouse on Databricks

Bronze → Silver → Gold **Delta Lake** pipeline on **Databricks Free Edition**, built with
**PySpark + Spark SQL**: incremental **MERGE** upserts, **OPTIMIZE / Z-ORDER** tuning,
SQL **data-quality gates**, and scheduled as a **Databricks Workflow**.

**Dataset:** NYC TLC Yellow Taxi trips (public parquet) — tens of millions of rows.

## Résumé bullets this project backs
- Python/SQL/PySpark ETL/ELT that standardizes raw feeds into query-ready datasets, recurring daily — **Jio #1**
- SQL data-quality / validation checks that catch upstream discrepancies before reporting — **Jio #4**
- Dimensional modeling (fact + dimensions) for efficient downstream querying — **Jio #3 (partial)**
- Orchestrated scheduled workflows + Git — **Jio #5 (partial, via Databricks Workflows)**

(Kafka, Snowflake, ADLS, and Airflow are covered by Project 2.)

## Architecture
See [docs/architecture.md](docs/architecture.md). TODO: paste the diagram + a screenshot of the Workflow run.

## Stack
Databricks Free Edition · PySpark · Delta Lake · Spark SQL · GitHub Actions (CI)

## How to run
TODO — fill this in as you build (an interviewer will try to run it):
1. ...
2. ...

## Repo layout
- `notebooks/` — Databricks notebooks: bronze → silver → gold → dq
- `src/`       — pure transform + DQ functions (unit-tested in CI)
- `tests/`     — pytest
- `scripts/`   — data download helper
- `.github/workflows/` — CI (lint + tests)
- `docs/`      — architecture + design decisions

## Design decisions (interviewers read this)
TODO: why Delta, gold grain, partition vs Z-ORDER, MERGE semantics, how DQ gates the job.
