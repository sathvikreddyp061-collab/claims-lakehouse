# Architecture

## One-line summary

10,000 synthetic Synthea patients → ~250K mocked EDI 837 claims → Kafka →
PySpark bronze on Iceberg → dbt-DuckDB silver+gold marts gated by Great
Expectations → Postgres mirror for a Next.js member-360 dashboard. The whole
pipeline runs on a laptop via Docker Compose and is composed by a single
Airflow DAG that can be triggered or scheduled.

## Component map

| Layer | Component | Why this choice |
|---|---|---|
| Patient population | **Synthea** | Industry-standard synthetic FHIR/CSV generator. Reproducible from a seed; output cached locally so we don't regenerate on every dbt run. |
| Claims wire format | **Custom X12 EDI 837 generator** | Real payers exchange 837s, not JSON. `producer/edi/x12_837.py` round-trip writes and parses the minimum-viable 837P subset, tested in `tests/test_edi_roundtrip.py`. |
| Transport | **Redpanda** (Kafka API) | Same choice as `fraud-streaming` — single binary, no ZooKeeper. Producer publishes JSON-wrapped claims (the EDI body is one field). |
| Bronze landing | **PySpark Structured Streaming** → **Iceberg on MinIO** | Hidden partitioning by `service_month` (claims are bursty by service date). Time-travel + schema evolution land for free. |
| Silver / Gold | **dbt-duckdb** | DuckDB runs in-process; no warehouse infra to manage. dbt project structured so swapping the profile to Snowflake is a 1-file change. |
| Data contracts | **Great Expectations** | `claims_silver` suite is the hard gate between silver and gold. Airflow fails the run on any violation — no silent regressions. |
| HIPAA | **dbt `hipaa_mask` macro** + `gold` schema isolation | SHA-256 mask on PII-derived columns; raw PII never leaves the silver layer. Salt is env-var-pluggable. |
| Orchestration | **Airflow 2.9** | Single DAG composes `synthea_generate → edi_build → kafka_produce → wait_bronze → dbt_build → ge_gate → pg_mirror`. |
| Dashboard | **Next.js 14 (App Router)** on **Vercel**, backed by **Neon Postgres** | Same surface pattern as `fraud-streaming` — local pipeline writes Iceberg, gold marts mirror to a managed Postgres, Vercel reads from there. |
| Observability | **Prometheus + Grafana** | Producer EPS, Airflow SLA, dbt run duration, GE pass/fail counts. |

## Data model

```
bronze.claims_edi837         (Iceberg, monthly-partitioned, raw + ingest metadata)
        │
        ▼
silver.claims_enriched       (dbt, typed columns + derived dollar_band/setting)
        │           │
        │           ▼
        │       Great Expectations gate    ← Airflow blocks gold on failure
        │
        ▼
gold.member_360              (per-member 90-day utilization, masked PII)
gold.claims_daily            (daily volume + paid by plan)
        │
        ▼
postgres.gold.{member_360, claims_daily}    ← pg_mirror sync at end of DAG
        │
        ▼
Next.js dashboard on Vercel
```

## What's deliberately *not* here yet

- **Real eligibility 270/271** roundtrip — the EDI work here is 837 only; eligibility joins use a Synthea-derived seed.
- **Continuous CDC** — the producer batch-publishes the full EDI manifest; in production the cadence would be webhook-driven from the clearinghouse.
- **Row-level security** in the gold mirror — the `hipaa_mask` macro covers the PII story; per-tenant RLS lives in a v2 follow-up alongside `pg_mirror` privilege grants.

See [`docs/adr/`](./adr/) for individual decisions.
