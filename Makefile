SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env
PY := .venv/bin/python
PIP := .venv/bin/pip
DBT := .venv/bin/dbt

ifneq (,$(wildcard .env))
include .env
export
endif

## help: list targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed -e 's/## //'

## venv: create local Python venv (needs Python 3.11 or 3.12 on PATH)
venv:
	@if [ ! -d .venv ]; then \
		for v in python3.12 python3.11 python3; do \
			if command -v $$v >/dev/null 2>&1; then \
				ver=$$($$v -c "import sys; print(f'{sys.version_info[0]}{sys.version_info[1]}')"); \
				if [ $$ver -ge 311 ] && [ $$ver -le 312 ]; then PYBIN=$$v; break; fi; \
			fi; \
		done; \
		if [ -z "$$PYBIN" ]; then \
			echo "ERROR: need Python 3.11 or 3.12 on PATH (install via pyenv or uv)"; exit 1; \
		fi; \
		echo "creating venv with $$PYBIN"; $$PYBIN -m venv .venv; \
	fi
	@$(PIP) install --quiet --upgrade pip
	@$(PIP) install --quiet -e ".[dev]"
	@echo "venv ready ($$($(PY) --version))"

## env: copy .env.example -> .env if missing
env:
	@test -f .env || (cp .env.example .env && echo "wrote .env")

## up: boot the local platform (Redpanda, MinIO, Postgres, Spark, Airflow, Prom, Grafana)
up: env
	$(COMPOSE) up -d
	@echo
	@echo "  Redpanda Console : http://localhost:8088"
	@echo "  MinIO Console    : http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Airflow UI       : http://localhost:8081  (admin / admin)"
	@echo "  Spark Master UI  : http://localhost:8080"
	@echo "  Prometheus       : http://localhost:9090"
	@echo "  Grafana          : http://localhost:3001  (admin / admin)"
	@echo
	@echo "Next: make synthea && make edi && make produce && make stream && make dbt"

## down: stop everything
down:
	$(COMPOSE) down

## nuke: stop and wipe volumes (destructive)
nuke:
	$(COMPOSE) down -v

## ps: show running containers
ps:
	$(COMPOSE) ps

## logs: tail compose logs
logs:
	$(COMPOSE) logs -f --tail=100

## topics: create Kafka topics
topics:
	@docker exec cl-redpanda rpk topic create claims.edi837.v1 -p 6 -r 1 -c retention.ms=604800000 || true
	@docker exec cl-redpanda rpk topic create eligibility.v1 -p 3 -r 1 || true
	@docker exec cl-redpanda rpk topic create member.events.v1 -p 3 -r 1 || true
	@docker exec cl-redpanda rpk topic list

## synthea: generate Synthea-shaped synthetic patients (Python, no JVM needed)
## See ADR-0003 for why we run our own generator instead of the Synthea image.
synthea: venv
	$(PY) -m producer.synthea.generator
	@ls data/synthea_output/csv/ 2>/dev/null | head -10 || echo "no output yet"

## edi: convert Synthea claims → EDI 837 mock files
edi: venv
	$(PY) -m producer.edi.x12_837_writer

## produce: stream EDI 837 events to Kafka
produce: venv
	$(PY) -m producer.kafka_publisher

## stream: submit Spark bronze ingest job
stream:
	$(COMPOSE) exec -T spark /opt/spark/bin/spark-submit \
		--master "local[2]" \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
		--conf spark.pyspark.python=python3 \
		--conf spark.driver.memory=2g \
		/workspace/streaming/bronze.py

## dbt: refresh bronze → DuckDB, then run silver + gold dbt models
dbt: venv
	$(PY) -m scripts.refresh_bronze
	cd dbt && $(DBT) deps
	cd dbt && $(DBT) build --profiles-dir .

## dbt-docs: generate + serve dbt docs lineage UI
dbt-docs: venv
	cd dbt && $(DBT) docs generate --profiles-dir .
	cd dbt && $(DBT) docs serve --profiles-dir . --port 8082

## ge: run Great Expectations validation
ge: venv
	$(PY) -m scripts.run_ge

## dag: trigger the end-to-end Airflow DAG
dag:
	docker exec cl-airflow airflow dags trigger claims_pipeline

## dashboard: boot the Next.js member-360 dashboard (Vercel target)
dashboard:
	cd dashboard && npm install --silent && npm run dev

## test: run unit + integration tests
test: venv
	$(PY) -m pytest -q

## lint: ruff + mypy
lint: venv
	.venv/bin/ruff check .
	.venv/bin/mypy producer streaming

## fmt: format with ruff
fmt: venv
	.venv/bin/ruff format .
	.venv/bin/ruff check --fix .

.PHONY: help venv env up down nuke ps logs topics synthea edi produce stream dbt dbt-docs ge dag dashboard test lint fmt
