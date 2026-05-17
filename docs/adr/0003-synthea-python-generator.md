# ADR-0003: Python "Synthea-shaped" generator over the official Synthea image

**Status:** Accepted · 2026-05-17

## Context

The repo was originally scaffolded to run the upstream Synthea JAR via Docker.
First verification attempt against `intersystemsdc/irisdemo-base-synthea:version-1.3.4`
on Apple Silicon (arm64) failed two ways:

1. Image is `linux/amd64`-only — Docker emulates via QEMU, slow + flaky.
2. The image's entrypoint (`/synthea/bin/synthea`) NPE-crashes on `--help` and
   any combination of args we tried. Even a fresh `docker run` couldn't get
   past property parsing.

The MITRE-published synthea image (`mitre/synthea`) doesn't exist on Docker Hub
either as of 2026-05. Running the JAR directly on the host requires a JDK
install + a 250 MB download — friction for a portfolio demo that should
work from a single `make` command.

## Options considered

1. **Bake a custom Synthea image** — pull synthea-with-dependencies.jar in a
   Dockerfile, pin it. Maintenance burden every Synthea release.
2. **Require Java on the host** — works but raises the "clone and run" bar.
3. **Python generator producing CSVs with the Synthea schema** — same column
   names, same semantics, runs in ~5 seconds at 10K patients, no external
   dependency. `producer.synthea.reader` doesn't know the difference.

## Decision

**Option 3.** `producer/synthea/generator.py` produces `patients.csv`,
`encounters.csv`, `claims.csv`, `providers.csv`, `payers.csv`, `organizations.csv`
with column sets that match Synthea v3.x (Synthea CSV File Data Dictionary).
Deterministic given a seed; cross-platform.

## Consequences

- **`make synthea`** now takes seconds, not minutes. The DAG step renames but
  the SLA gets easier.
- **What the generator does NOT produce yet**: medications, immunizations,
  observations, conditions, careplans — Synthea generates those too. We don't
  use them downstream; if a future v2 mart needs them, extending the generator
  is a localized change.
- **Loss**: clinical realism around disease modules. Synthea models actual
  disease progression; our generator samples uniformly from MCC/ICD codes.
  This is fine for the portfolio's data-engineering story (which is about
  the pipeline, not the medicine).
