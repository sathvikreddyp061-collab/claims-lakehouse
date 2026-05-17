# ADR-0001: dbt-duckdb over Snowflake trial for the local warehouse

**Status:** Accepted · 2026-05-16

## Context

The dbt project needs a warehouse runtime that (a) works on a laptop with zero
infra cost, (b) doesn't expire from under the demo, and (c) lets us swap to a
real cloud warehouse later without rewriting models.

## Options considered

1. **Snowflake free trial** — 30 days then the demo silently breaks. Most
   realistic for a healthcare-payer story (Snowflake is the de facto default),
   but a portfolio that stops working in a month is worse than no portfolio.
2. **Postgres** — already in the compose stack for the Vercel mirror. Works
   fine for the scale but commits dbt to a row-store, not a typical analytics
   warehouse pattern.
3. **DuckDB via dbt-duckdb** — embedded analytics database, columnar,
   single-file storage, runs against the Iceberg lake via the `iceberg`
   extension. Zero infra; identical SQL surface to most warehouses for the
   transforms we care about.

## Decision

**dbt-duckdb 1.8.1**. Profile structured so swapping to Snowflake is a
`profiles.yml` outputs swap; macros (including `hipaa_mask`) use dialect-neutral
SQL that runs on both.

## Consequences

- `make dbt` runs in seconds against a single `.duckdb` file (~30MB at 10K
  patients), keeping iteration fast.
- The DuckDB file is *not* committed to git (it's regenerated on every run);
  the `.duckdb.wal` from the runtime is gitignored alongside it.
- Cloud-target audit: any model using DuckDB-only functions (e.g. `regexp_extract`)
  needs a corresponding Snowflake alternative when we move. Today only one such
  call exists, in `stg_claims_837`.
