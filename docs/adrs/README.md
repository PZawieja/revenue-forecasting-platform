# Architecture Decision Records (ADRs)

## What are ADRs?

Architecture Decision Records are short, immutable documents that capture an important architectural decision, its context, and consequences. They give future staff and principals a clear record of *why* the system is built the way it is, so we can revisit or change course with full context.

## How to add a new ADR

1. Copy `0000-template.md` to a new file using the naming convention below.
2. Fill in each section: Context, Decision, Alternatives considered, Consequences, How to revisit.
3. Keep it concise and senior-level: tradeoffs explicit, no fluff.
4. Open a PR; ADRs are reviewed like code.

## Naming convention

Use the pattern: **`NNNN-title-with-dashes.md`**

- **NNNN** — Zero-padded number (e.g. `0001`, `0002`, `0012`). Next number = highest existing + 1.
- **title** — Short, descriptive, lowercase, words separated by hyphens.

Examples: `0001-deterministic-baseline-first.md`, `0002-ml-as-calibration-modules.md`.

## Index

| ADR | Title |
|-----|--------|
| [0001](0001-deterministic-baseline-first.md) | Deterministic baseline first |
| [0002](0002-ml-as-calibration-modules.md) | ML as calibration modules |
| [0003](0003-duckdb-dbt-portability.md) | DuckDB + dbt for portability |
