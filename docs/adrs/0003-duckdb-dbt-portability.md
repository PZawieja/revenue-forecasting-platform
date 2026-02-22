# ADR 0003: DuckDB + dbt for portability

**Status:** Accepted  
**Date:** 2025-02-22

## Context

We need an analytical store that supports local development, demos, CI, and optional production. The team prefers SQL-centric transformations, version-controlled logic, and minimal infra dependency for the MVP.

## Decision

We use **DuckDB** as the analytical database and **dbt** for all transformations (staging → intermediate → marts). DuckDB is file-based (single `*.duckdb` in `warehouse/`); no external server. dbt runs with a repo-local profile (`dbt/profiles`); paths are relative so the project runs from any clone. Python (ML, export, Streamlit) reads/writes the same DuckDB file. We do not introduce a second database for the core pipeline.

## Alternatives considered

- **Postgres / Snowflake / BigQuery:** Would improve collaboration and scale but add infra, credentials, and cost; rejected for MVP to maximize portability and “run anywhere” demos.
- **SQLite:** Simpler but weaker analytics and no native dbt adapter; DuckDB gives better SQL and performance for analytics.
- **Parquet-only + Pandas:** No single query interface; harder for golden queries and ad-hoc analysis; rejected.

## Consequences

- **Positive:** One-command demo (make demo), no DB server, CI runs full dbt + ML against DuckDB; golden queries and cockpit use the same marts.
- **Negative:** File-based DB is not ideal for concurrent writes or very large data; we accept single-writer and “copy file” for backups/snapshots.
- **Neutral:** If we later add Postgres/Snowflake, we’d replicate the dbt project and swap the profile; DuckDB remains the reference for local/CI.

## How to revisit

Revisit when we need multi-user writes, real-time ingestion, or cloud-scale querying; then consider a dedicated warehouse with the same dbt models and a new profile.
