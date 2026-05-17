# claims-lakehouse

> HIPAA-grade claims + member analytics pipeline at synthetic-payer scale.
> Synthea → EDI 837 → Kafka → PySpark bronze → dbt-DuckDB silver/gold → Great Expectations gates → Airflow.

This is the engineering portfolio companion to my **Healthcare Insurance** case study. The whole pipeline runs on a laptop via `docker compose`. A Next.js member-360 dashboard deploys to Vercel against a Postgres mirror of the gold marts.

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌────────────┐    ┌──────────────────────┐
│   Synthea    │───▶│  EDI 837     │───▶│  Redpanda  │───▶│  PySpark bronze     │
│ (10K members)│    │  generator   │    │  (Kafka)   │    │  → Iceberg (MinIO)  │
└──────────────┘    └──────────────┘    └────────────┘    └──────────┬───────────┘
                                                                      │
                                                                      ▼
                                                          ┌──────────────────────┐
                                                          │  Great Expectations  │
                                                          │  hard gate in CI     │
                                                          └──────────┬───────────┘
                                                                      ▼
                                                          ┌──────────────────────┐
                                                          │  dbt-DuckDB silver   │
                                                          │  → gold member-360   │
                                                          └──────────┬───────────┘
                                                                      ▼
                                ┌─────────────────────────────────────┴────────────────────┐
                                ▼                                                            ▼
                       ┌──────────────────┐                                       ┌──────────────────┐
                       │  Postgres mirror │                                       │   dbt docs       │
                       │  (Vercel reads)  │                                       │  lineage UI      │
                       └────────┬─────────┘                                       └──────────────────┘
                                ▼
                       ┌──────────────────┐
                       │ Next.js (Vercel) │
                       │  member-360 view │
                       └──────────────────┘
```

Orchestration: an **Airflow** DAG composes Synthea generation, the EDI-837 producer, the bronze consumer, GE validation, and `dbt run` + `dbt test` as a single end-to-end run.

## Why this repo exists

To prove the claims on my portfolio with code you can run:

| Claim | Backed by |
|---|---|
| 2M+ members served (synthetic 10K here) | `producer/synthea/` generator |
| EDI 837 claims feed | `producer/edi/x12_837.py` generator + parser, round-trip tested |
| 250K daily claims processed | `airflow/dags/claims_pipeline.py` shape × dbt incremental |
| HIPAA: lineage + masking | `dbt/macros/hipaa_mask.sql` + dbt docs |
| Great Expectations DQ gates | `great_expectations/expectations/` + Airflow sensor |
| 30% fewer SLA misses (vs SSIS) | Airflow run-time budget + drift detector |
| Member-360 marts → BI | `dbt/models/gold/member_360.sql` + Next.js dashboard |

## Quick start

**Prereqs:** Docker Desktop running, Python **3.11 or 3.12** on `PATH` (install via `pyenv install 3.12` or `uv python install 3.12`).

```bash
make up          # boots Redpanda, MinIO, Postgres, Spark, Airflow
make synthea     # generates 10K patients, 2yr history (cached after first run)
make edi         # converts Synthea claims → EDI 837 mock files
make produce     # streams EDI 837 events to Kafka
make stream      # submits the Spark bronze ingest job
make dbt         # runs silver + gold dbt models on DuckDB
make ge          # runs Great Expectations validation
make dag         # runs the full Airflow DAG end-to-end
make dashboard   # boots the Next.js member-360 dashboard on :3000
make down        # tears it all down
```

Service URLs after `make up`:

| URL | Purpose |
|---|---|
| http://localhost:8088 | Redpanda Console — browse topics + EDI 837 messages |
| http://localhost:9001 | MinIO Console — inspect the Iceberg lake (`minioadmin` / `minioadmin`) |
| http://localhost:8081 | Airflow UI (`admin` / `admin`) |
| http://localhost:8080 | dbt docs (after `make dbt-docs`) |
| http://localhost:9090 | Prometheus |
| http://localhost:3001 | Grafana (admin / admin) |

## Project layout

```
claims-lakehouse/
├── producer/
│   ├── synthea/        # Synthea Docker runner + parser
│   └── edi/            # EDI 837 X12 generator + Kafka publisher
├── streaming/          # PySpark bronze ingest (EDI → Iceberg)
├── dbt/
│   ├── models/
│   │   ├── staging/    # 1:1 source models on bronze
│   │   ├── silver/     # claims_enriched, eligibility_history, member_dim
│   │   └── gold/       # member_360, claims_summary, drg_costs
│   ├── macros/         # hipaa_mask, rls_helpers
│   └── tests/          # data contracts
├── great_expectations/ # baseline suite for EDI 837, member, claim
├── airflow/dags/       # end-to-end DAG (Synthea → … → dbt → GE)
├── infra/              # docker-compose, init scripts, prometheus config
├── dashboard/          # Next.js member-360 view (Vercel)
├── scripts/            # smoke tests, fixture loaders
└── docs/               # ADRs, runbooks, screenshots
```

## Tech stack

**Streaming:** Redpanda (Kafka API), JSON-encoded EDI 837 payloads
**Compute:** PySpark 3.5 + Iceberg 1.5
**Storage:** MinIO (S3 API) for the lake, Postgres for the Vercel mirror, DuckDB for the warehouse
**Analytics:** **dbt-duckdb** with tests + docs + lineage UI
**Quality:** Great Expectations (Avro + EDI 837 + warehouse layers)
**Orchestration:** **Airflow 2.9** (LocalExecutor)
**Dashboard:** Next.js 14, deployed to Vercel
**Observability:** Prometheus + Grafana, OpenLineage to Marquez

## Roadmap

- [ ] Repo scaffold + docker-compose
- [ ] Synthea Docker runner + CSV parser
- [ ] EDI 837 X12 generator + parser + round-trip test
- [ ] Kafka producer streaming EDI 837
- [ ] PySpark bronze ingest → Iceberg
- [ ] dbt staging → silver → gold marts on DuckDB
- [ ] Great Expectations baseline suite + Airflow gate
- [ ] HIPAA dbt macros (hash masking, row-level filters)
- [ ] dbt docs lineage UI committed as artifact
- [ ] Airflow end-to-end DAG with SLA monitoring
- [ ] Next.js member-360 dashboard
- [ ] Postgres mirror sync job for the Vercel surface

## License

MIT
